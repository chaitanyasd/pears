import aiohttp
import logging
import bencodepy
import random
import socket

from urllib.parse import urlencode


class TrackerResponse:
    """
    https://wiki.theory.org/BitTorrentSpecification#Tracker_Response
    """

    def __init__(self, tracker_response):
        self.tracker_response = tracker_response

    @property
    def failure(self):
        if b"failure reason" in self.tracker_response:
            return self.tracker_response[b"failure reason"].decode("utf-8")
        return None

    @property
    def warning(self):
        if b"warning message" in self.tracker_response:
            return self.tracker_response[b"warning message"].decode("utf-8")
        return None

    @property
    def interval(self) -> int:
        return self.tracker_response.get(b"interval", 0)

    @property
    def complete(self) -> int:
        return self.tracker_response.get(b"complete", 0)

    @property
    def incomplete(self) -> int:
        return self.tracker_response.get(b"incomplete", 0)

    @property
    def peers(self):
        peers = self.tracker_response[b"peers"]

        if type(peers) == list:
            logging.info("The peers list is a list of dict (dictionary model)")
            raise NotImplementedError("The peers list is a list of dict (dictionary model)")
        else:
            logging.info("The peers list is string (binary model)")
            peers = [
                peers[i:i + 6]
                for i in range(0, len(peers), 6)
            ]

            return [
                (socket.inet_ntoa(ip[:4]), self._decode_port(ip[4:]))
                for ip in peers
            ]

    def _decode_port(self, port):
        import struct
        return struct.unpack(">H", port)[0]

    def __str__(self):
        return f"incomplete (leechers): {self.incomplete}\n" \
               f"complete (peers): {self.complete}\n" \
               f"interval (sec): {self.interval}\n" \
               f"peers ip:port: {self.peers}\n"


class Tracker:
    def __init__(self, torrent):
        self.torrent = torrent
        self.http_client = aiohttp.ClientSession()
        self.peer_id = self._calculate_peer_id()

    async def connect(self, first: bool = None, uploaded: int = 0, downloaded: int = 0):
        params = self._get_request_params(first, uploaded, downloaded)

        tracker_url = self.torrent.announce + '?' + urlencode(params)
        logging.info(f"Connecting to tracker at:: {tracker_url} ")

        async with self.http_client.get(tracker_url) as response:
            if not response.status == 200:
                raise ConnectionError(f"Unable to connect to tracker: status code {response.status}")
            tracker_response = await response.read()
            self.check_for_error(tracker_response)
            return TrackerResponse(bencodepy.decode(tracker_response))

    def _calculate_peer_id(self):
        return "-EZ9426-" + "".join([str(random.randint(0, 9)) for _ in range(12)])

    def check_for_error(self, tracker_response):
        try:
            message = tracker_response.decode("utf-8")
            if "failure" in message:
                raise ConnectionError(f"Unable to connect to tracker: {message}")
        except UnicodeDecodeError:
            pass

    def close(self):
        self.http_client.close()

    def _get_request_params(self, first: bool = None, uploaded: int = 0, downloaded: int = 0):
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'port': 6889,  # try changing this??
            'left': self.torrent.total_length - downloaded,
            'compact': 1
        }

        # ?? find its use
        if first:
            params['event'] = 'started'

        return params
