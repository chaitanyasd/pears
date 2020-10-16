import asyncio
import math
import os
import time
from collections import defaultdict, namedtuple
from hashlib import sha1
from typing import List, Union
from logger import init_logger
from protocol import PeerConnection, REQUEST_SIZE
from tracker import Tracker

MAX_PEER_CONNECTIONS = 40
PendingRequest = namedtuple("PendingRequest", ['block', 'added'])
logging = init_logger(__name__, testing_mode=False)


class TorrentClient:
    def __init__(self, torrent):
        self.tracker = Tracker(torrent)
        self.piece_manager = PieceManager(torrent)
        self.available_peers = asyncio.Queue()
        self.peers = []
        self.abort = False

    async def start(self):
        self.peers = [
            PeerConnection(
                self.available_peers,
                self.tracker.torrent.info_hash,
                self.tracker.peer_id,
                self.piece_manager,
                self._on_block_retrieved
            )
            for _ in range(MAX_PEER_CONNECTIONS)
        ]

        previous_request = None  # timestamp
        request_interval = 1800  # sec

        while True:
            if self.abort:
                logging.log("Aborting")
                break

            if self.piece_manager.complete:
                logging.info("Download completed")
                break

            current_time = time.time()

            if (not previous_request) or (previous_request + request_interval) < current_time:
                tracker_response = await self.tracker.connect(
                    first=previous_request if previous_request else False,
                    uploaded=self.piece_manager.bytes_uploaded,
                    downloaded=self.piece_manager.bytes_downloaded
                )

                if tracker_response:
                    previous_request = current_time
                    request_interval = tracker_response.interval
                    self._empty_queue()
                    for peer in tracker_response.peers:
                        self.available_peers.put_nowait(peer)
            else:
                await asyncio.sleep(5)
        self.stop()

    def stop(self):
        self.abort = True
        for peer in self.peers:
            peer.stop()
        self.piece_manager.close()
        self.tracker.close()

    def _empty_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        self.piece_manager.block_received(
            peer_id=peer_id,
            piece_index=piece_index,
            block_offset=block_offset,
            data=data
        )


class Block:
    # todo: convert to enum
    Missing = 0
    Pending = 1
    Retrieved = 2

    def __init__(self, piece: int, offset: int, length: int):
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data = None


class Piece:
    def __init__(self, index: int, blocks: List[Block] = None, hash_value=None):
        self.index = index
        self.blocks = blocks if blocks else []
        self.hash_value = hash_value

    def reset(self) -> None:
        for block in self.blocks:
            block.status = Block.Missing

    def is_complete(self) -> bool:
        blocks = [block for block in self.blocks if block.status is not Block.Retrieved]
        return len(blocks) == 0

    def next_request(self) -> Union[Block, None]:
        missing_block = [block for block in self.blocks if block.status == Block.Missing]
        if missing_block:
            missing_block[0].status = Block.Pending
            return missing_block[0]
        return None

    def block_received(self, offset: int, data: bytes) -> None:
        _block = [block for block in self.blocks if block.offset == offset]
        block = _block[0] if _block else None

        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning("Received a non-existing block {} in piece {}".format(offset, self.index))

    def is_hash_matching(self) -> bool:
        piece_hash = sha1(self.data).digest()
        return self.hash_value == piece_hash

    @property
    def data(self) -> bytes:
        piece_data = sorted(self.blocks, key=lambda block: block.offset)
        piece_data = [piece.data for piece in piece_data]
        return b''.join(piece_data)


class PieceManager:
    def __init__(self, torrent):
        self.torrent = torrent
        self.peers = {}
        self.pending_blocks = []
        self.have_pieces = []
        self.ongoing_pieces = []
        self.total_pieces = len(torrent.pieces)
        self.max_pending_time = 300 * 1000  # 5 minutes
        self.fd = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)
        self.missing_pieces = self._init_pieces()

    def _init_pieces(self) -> List[Piece]:
        pieces = []
        num_std_blocks = math.ceil(self.torrent.piece_length / REQUEST_SIZE)

        for piece_index, piece_hash in enumerate(self.torrent.pieces):
            if piece_index < (self.total_pieces - 1):
                blocks = [
                    Block(piece_index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_std_blocks)
                ]
            else:
                last_piece_length = self.torrent.total_length % self.torrent.piece_length
                num_last_blocks = math.ceil(last_piece_length / REQUEST_SIZE)
                blocks = [
                    Block(piece_index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_last_blocks)
                ]

                last_block_length = last_piece_length % REQUEST_SIZE
                if last_block_length > 0:
                    last_block = blocks[-1]
                    last_block.length = last_block_length
                    blocks[-1] = last_block
            pieces.append(Piece(piece_index, blocks, piece_hash))
        return pieces

    @property
    def complete(self) -> bool:
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_uploaded(self) -> int:
        # todo
        return 0

    @property
    def bytes_downloaded(self) -> int:
        return len(self.have_pieces) * self.torrent.piece_length

    def close(self) -> None:
        if self.fd:
            os.close(self.fd)

    def add_peer(self, peer_id, bitfield):
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        del self.peers[peer_id]

    def block_received(self, peer_id, piece_index, block_offset, data):
        logging.info('Received block {block_offset} for piece {piece_index} '
                     'from peer {peer_id}: '.format(block_offset=block_offset,
                                                    piece_index=piece_index,
                                                    peer_id=peer_id))

        # remove the block from pending blocks
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece == piece_index and \
                    request.block.offset == block_offset:
                del self.pending_blocks[index]
                break

        piece = [piece for piece in self.ongoing_pieces if piece.index == piece_index]
        piece = piece[0] if piece else None

        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)

                    total_complete = self.total_pieces - len(self.ongoing_pieces) - len(self.missing_pieces)
                    logging.info(
                        '...................... {complete} / {total} pieces downloaded {per:.3f}% '
                        '......................% '
                            .format(complete=total_complete,
                                    total=self.total_pieces,
                                    per=(total_complete / self.total_pieces) * 100))
                else:
                    logging.info('Discarding corrupt piece {index}'
                                 .format(index=piece.index))
                    piece.reset()
        else:
            logging.warning('Trying to update a piece which is not ongoing!')

    def next_request(self, peer_id) -> Union[Block, None]:
        if peer_id not in self.peers:
            return None

        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                block = self._get_rarest_piece(peer_id).next_request()
        return block

    def _expired_requests(self, peer_id) -> Union[Block, None]:
        current_time = int(round(time.time() * 1000))

        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current_time:
                    logging.info('Re-requesting block {block} for '
                                 'piece {piece}'.format(
                        block=request.block.offset,
                        piece=request.block.piece))

                    request.added = current_time
                    return request.block
        return None

    def _next_ongoing(self, peer_id) -> Union[Block, None]:
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                block = piece.next_request()
                if block:
                    current_time = int(round(time.time() * 1000))
                    self.pending_blocks.append(PendingRequest(block, current_time))
                return block
        return None

    def _get_rarest_piece(self, peer_id) -> Piece:
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue

            for peer in self.peers:
                if self.peers[peer][piece.index]:
                    piece_count[piece] += 1

        rarest_piece = min(piece_count, key=lambda piece: piece_count[piece])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _write(self, piece):
        pos = self.torrent.piece_length * piece.index
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)

    def _next_missing(self, peer_id) -> Union[Block, None]:
        for index, piece in enumerate(self.missing_pieces):
            if self.peers[peer_id][piece.index]:
                piece = self.missing_pieces.pop(index)
                self.ongoing_pieces.append(piece)
                return piece.next_request()
        return None
