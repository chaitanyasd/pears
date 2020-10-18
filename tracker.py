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

import random
import socket
from urllib.parse import urlencode

import aiohttp
import bencodepy

from logger import init_logger, debug_logging_enabled

logging = init_logger(__name__, testing_mode=debug_logging_enabled)


class Tracker:
    """
    Responsible for connection management with Tracker
    More info: https://wiki.theory.org/index.php/BitTorrentSpecification#Tracker_Response
    """

    def __init__(self, torrent):
        self.torrent = torrent
        self.http_client = aiohttp.ClientSession(trust_env=True)
        self.peer_id = self._calculate_peer_id()

    async def connect(self, first: bool = None, uploaded: int = 0, downloaded: int = 0):
        """
        Make announcement call to the tracker with the current client torrent stats.
        If successful we will receive a response containing the peer list to connect to
        for downloading torrent data.
        TrackerResponse wraps the received data to be used by client
        """
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
        """
        https://wiki.theory.org/BitTorrentSpecification#peer_id
        """
        return "-EZ1426-" + "".join([str(random.randint(0, 9)) for _ in range(12)])

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
        """
        Returns the URL request parameters to be sent to tracker
        """
        params = {
            'info_hash': self.torrent.info_hash,
            'peer_id': self.peer_id,
            'uploaded': uploaded,
            'downloaded': downloaded,
            'port': 6889,  # try changing this??
            'left': self.torrent.total_length - downloaded,
            'compact': 1
        }

        if first:
            params['event'] = 'started'

        return params


class TrackerResponse:
    """
    TrackerResponse holds the methods to act on the response we get from the tracker's
    announce URL
    More info: https://wiki.theory.org/BitTorrentSpecification#Tracker_Response
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
        """
        The interval in seconds that the client should wait before sending
        periodic calls to the tracker
        """
        return self.tracker_response.get(b"interval", 0)

    @property
    def complete(self) -> int:
        """
        Number of peers with complete file i.e seeders
        """
        return self.tracker_response.get(b"complete", 0)

    @property
    def incomplete(self) -> int:
        """
        Number of non-seeder peers i.e leechers
        """
        return self.tracker_response.get(b"incomplete", 0)

    @property
    def peers(self):
        """
        A list of tuples each representing a peer (ip, port)
        """
        peers = self.tracker_response[b"peers"]

        if type(peers) == list:
            logging.info("The peers list is a list of dict (dictionary model)")
            raise NotImplementedError("The peers list is a list of dict (dictionary model)")
        else:
            # Split the data into 6 bytes, where first 4 bytes indicate the peer ip
            # and the last 2 bytes is peer's TCP port number
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
