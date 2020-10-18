"""
pears: A simple BitTorrent client

MIT License

Copyright (c) 2020 Chaitanya Deshpande

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import asyncio
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from hashlib import sha1
from typing import List, Union

from logger import init_logger, debug_logging_enabled
from protocol import PeerConnection, REQUEST_SIZE
from tracker import Tracker

# Maximum number of workers who can connect to peers
MAX_PEER_CONNECTIONS = 40
logging = init_logger(__name__, testing_mode=debug_logging_enabled)


class TorrentClient:
    """
    TorrentClient is a local peer which is responsible for managing connections
    to other peers to download and upload the data for the torrent.

    It makes periodic calls to the tracker registered to the torrent meta-info to get peer
    information for the torrent.

    Each received peer is stored in the queue from which the PeerConnection objects consume
    (maximum peer connections is defined by global MAX_PEER_CONNECTIONS variable).
    """

    def __init__(self, torrent):
        # Tracker object that defines methods to connect to the tracker
        self.tracker = Tracker(torrent)
        self.piece_manager = PieceManager(torrent)
        # Queue of potential peers which the PeerConnection objects will consume
        self.available_peers = asyncio.Queue()
        # List of PeerConnection objects which might be connected to the peer.
        # Else it is waiting to consume a peer from the available_peers queue
        self.peers = []
        self.abort = False

    async def start(self):
        """
        Start downloading the torrent data by connecting to the tracker and starting
        the workers
        """
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

        # Timestamp of the last announce call to the tracker
        previous_request = None
        # Default request interval (in sec) to tracker
        request_interval = 300  # sec

        while True:
            if self.abort:
                logging.warning("Aborting download")
                break

            if self.piece_manager.complete:
                logging.info("Download completed")
                break

            current_time = time.time()

            # Check if request_interval time has passed so make announce call to tracker
            if (not previous_request) or (previous_request + request_interval) < current_time:
                tracker_response = await self.tracker.connect(
                    first=previous_request if previous_request else False,
                    uploaded=self.piece_manager.bytes_uploaded,
                    downloaded=self.piece_manager.bytes_downloaded
                )

                if tracker_response:
                    logging.debug("Got tracker response")
                    previous_request = current_time
                    request_interval = tracker_response.interval
                    self._empty_queue()
                    for peer in tracker_response.peers:
                        if peer:
                            self.available_peers.put_nowait(peer)
            else:
                logging.warning("Waiting for next tracker announce call, interval is {request_interval}".format(
                    request_interval=request_interval))
                for peer in self.peers:
                    logging.debug("State of peer {id} is {status}".format(
                        id=peer.remote_id,
                        status=peer.my_state))
                await asyncio.sleep(5)
        self.stop()

    def stop(self):
        """
        Stop download and send stop signal to all peer objects
        """
        self.abort = True
        for peer in self.peers:
            peer.stop()
        self.piece_manager.close()
        self.tracker.close()

    def _empty_queue(self):
        """
        Remove all peers from the queue
        """
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
        Callback function passed to PeerConnection object called when block is
        retrieved from peer
        :param peer_id: The id of peer the block was retrieved from
        :param piece_index: The piece index of the block
        :param block_offset: Block offset inside the piece
        :param data: Binary block data
        :return:
        """
        self.piece_manager.block_received(
            peer_id=peer_id,
            piece_index=piece_index,
            block_offset=block_offset,
            data=data
        )


class Block:
    """
    A single piece if made up of several small blocks. A block is the smallest atomic unit
    transferred between peers.

    Block size if usually size of REQUEST_SIZE, except for the last block in a piece which maybe
    smaller than REQUEST_SIZE
    """
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
    """
    Torrent are made up of pieces which are made up of blocks. Each piece is of same
    size except for the last piece which might be smaller.
    """

    def __init__(self, index: int, blocks: List[Block] = None, hash_value=None):
        self.index = index
        self.blocks = blocks if blocks else []
        self.hash_value = hash_value

    def reset(self) -> None:
        """
        Reset status of all blocks in a piece to missing irrespective of current state
        """
        for block in self.blocks:
            block.status = Block.Missing

    def is_complete(self) -> bool:
        """
        Returns if all blocks of piece are downloaded
        """
        blocks = [block for block in self.blocks if block.status is not Block.Retrieved]
        return len(blocks) == 0

    def next_request(self) -> Union[Block, None]:
        """
        Get the next missing block of the piece
        """
        missing_block = [block for block in self.blocks if block.status == Block.Missing]
        if missing_block:
            missing_block[0].status = Block.Pending
            return missing_block[0]
        return None

    def block_received(self, offset: int, data: bytes) -> None:
        """
        Updates block information
        :param offset: Offset of block inside the piece
        :param data: Block data
        """
        _block = [block for block in self.blocks if block.offset == offset]
        block = _block[0] if _block else None

        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning("Received a non-existing block {} in piece {}".format(offset, self.index))

    def is_hash_matching(self) -> bool:
        """
        Returns if the piece hash (SHA-1) is same as the hash received from torrent meta-info
        """
        piece_hash = sha1(self.data).digest()
        return self.hash_value == piece_hash

    @property
    def data(self) -> bytes:
        """
        Returns data of the piece by concatenating all block data (sorting blocks
        in order of their offset)
        """
        piece_data = sorted(self.blocks, key=lambda block: block.offset)
        piece_data = [piece.data for piece in piece_data]
        return b''.join(piece_data)


class PieceManager:
    """
    PieceManager is responsible for managing and keeping track of all the pieces of
    the connected peers as well as other peers which might have been disconnected.
    """

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
        """
        Creates the list of pieces to be downloaded from the peers
        """
        pieces = []
        # Number of blocks in a piece if piece if of REQUEST_SIZE
        num_std_blocks = math.ceil(self.torrent.piece_length / REQUEST_SIZE)

        for piece_index, piece_hash in enumerate(self.torrent.pieces):
            if piece_index < (self.total_pieces - 1):
                blocks = [
                    Block(piece_index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_std_blocks)
                ]
            else:
                # Last piece length maybe small than previous ones
                last_piece_length = self.torrent.total_length % self.torrent.piece_length
                # Number of blocks in the last piece
                num_last_blocks = math.ceil(last_piece_length / REQUEST_SIZE)
                blocks = [
                    Block(piece_index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_last_blocks)
                ]

                # Update last block length in case it is smaller than the REQUEST_SIZE
                last_block_length = last_piece_length % REQUEST_SIZE
                if last_block_length > 0:
                    last_block = blocks[-1]
                    last_block.length = last_block_length
                    blocks[-1] = last_block
            pieces.append(Piece(piece_index, blocks, piece_hash))
        return pieces

    @property
    def complete(self) -> bool:
        """
        Checks if all pieces are received
        """
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_uploaded(self) -> int:
        """
        Not supported in this version of client
        """
        return 0

    @property
    def bytes_downloaded(self) -> int:
        """
        Returns number of bytes downloaded
        """
        return len(self.have_pieces) * self.torrent.piece_length

    def close(self) -> None:
        """
        Closes file descriptors opened by PieceManager
        """
        if self.fd:
            os.close(self.fd)

    def add_peer(self, peer_id, bitfield):
        """
        Adds a peer with bitfield representing the pieces available with that peer
        """
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        """
        Updates piece availability of the piece
        """
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        if peer_id in self.peers:
            del self.peers[peer_id]

    def block_received(self, peer_id, piece_index, block_offset, data):
        """
        Callback function called when a block is received.

        After receiving a block, it checks if the piece is complete. If yes,
        its hash is verified and the piece is written to disk and is added to
        have_pieces list. If hash is not verified, the piece is reset for re-download
        """
        logging.info('Received block {block_offset} for piece {piece_index} '
                     'from peer {peer_id}: '.format(block_offset=block_offset,
                                                    piece_index=piece_index,
                                                    peer_id=peer_id))

        # Remove the block from pending blocks
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
                        '\n................................................................\n'
                        '{complete} / {total} pieces downloaded {per:.3f}%\n'
                        '................................................................\n'
                            .format(complete=total_complete,
                                    total=self.total_pieces,
                                    per=(total_complete / self.total_pieces) * 100))
                else:
                    logging.warning('Discarding corrupt piece {index}'
                                    .format(index=piece.index))
                    piece.reset()
        else:
            logging.warning('Trying to update a piece which is not ongoing!')

    def next_request(self, peer_id) -> Union[Block, None]:
        """
        Returns the block that is to be requested next for that peer.
        1. Check if any expired block requests are present
        2. Get the next block from ongoing piece of that peer
        3. Get rarest piece the peer has and return its 1st missing block
        4. Get the next missing piece for the peer
        """
        if peer_id not in self.peers:
            return None

        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                piece = self._get_rarest_piece(peer_id)
                if piece:
                    return piece.next_request()
                block = self._next_missing(peer_id)
        return block

    def _expired_requests(self, peer_id) -> Union[Block, None]:
        """
        Go through the previously requested blocks and check if any request
        is pending since a long time (> max_pending_time). If yes, return that block
        """
        current_time = int(round(time.time() * 1000))

        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if current_time > (request.added + self.max_pending_time):
                    logging.info('Re-requesting block {block} for '
                                 'piece {piece}'.format(
                        block=request.block.offset,
                        piece=request.block.piece))

                    request.added = current_time
                    return request.block
        return None

    def _next_ongoing(self, peer_id) -> Union[Block, None]:
        """
        Go through the currently ongoing piece for the peer and return
        the next block for that piece
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                block = piece.next_request()
                if block:
                    current_time = int(round(time.time() * 1000))
                    self.pending_blocks.append(PendingRequest(block=block, added=current_time))
                return block
        return None

    def _get_rarest_piece(self, peer_id) -> Piece:
        """
        Go through all the missing pieces and check which is the piece that is rarest.
        If found, return that piece.
        This request strategy is good for keeping the seeder healthy.
        """
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue

            for peer in self.peers:
                if self.peers[peer][piece.index]:
                    piece_count[piece] += 1

        if len(piece_count) == 0:
            return None

        rarest_piece = min(piece_count, key=lambda piece: piece_count[piece])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _write(self, piece):
        """
        Write the piece data to the disk
        """
        logging.info("Writing piece {0} to disk....".format(piece.index))
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


@dataclass()
class PendingRequest:
    """
    A class representing a request made to peer
    block: Holds block object of the request
    added: Hold the timestamp of the request
    """
    block: Block = field()
    added: int = field()
