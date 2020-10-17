import argparse
from collections import namedtuple
from hashlib import sha1
import asyncio

import bencodepy

from tracker import Tracker
from logger import init_logger

"""
Represents each file within the torrent
"""
TorrentFile = namedtuple('TorrentFile', ['name', 'length'])
logging = init_logger(__name__, testing_mode=False)


class Torrent:
    """
    Represents torrent meta-data present in the .torrent file
    """

    def __init__(self, torrent_path):
        self.torrent_path = torrent_path
        self.files = []
        if self._validate_torrent_file():
            self.meta_info = bencodepy.bread(self.torrent_path)
            info = bencodepy.encode(self.meta_info[b"info"])
            self.info_hash = sha1(info).digest()
            self._get_torrent_files()

    def __str__(self):
        return f'Filename: {self.files[0].name}\n' \
               f'File length: {self.total_length}\n' \
               f'Announce URL: {self.announce}\n' \
               f'Hash: {self.info_hash}'

    def _validate_torrent_file(self) -> bool:
        """
        Validates the input file to check if it is a valid torrent file
        """
        import os
        try:
            if not os.path.isfile(self.torrent_path):
                raise RuntimeError(f"Exception: \"{self.torrent_path}\" is not a file")

            elif not self.torrent_path.endswith(".torrent"):
                raise RuntimeError(f"Exception: \"{self.torrent_path}\" is not a valid torrent file")
        except RuntimeError as e:
            print(e)
            return False

        return True

    def _get_torrent_files(self):
        """
        Returns the file present in the torrent. Currently it doesn't support torrents containing multiple files
        """
        if self.is_multi_file:
            raise RuntimeError("Torrent contains multiple files. This is not supported currently.")

        self.files.append(
            TorrentFile(
                name=self.meta_info[b"info"][b"name"].decode("utf-8"),
                length=self.meta_info[b"info"][b"length"]))

    @property
    def announce(self) -> str:
        """
        Returns the tracker URL
        """
        return self.meta_info[b"announce"].decode("utf-8")

    @property
    def is_multi_file(self) -> bool:
        """
        Returns if the torrent consists of multiple files
        """
        return b"files" in self.meta_info[b"info"]

    @property
    def piece_length(self) -> int:
        """
        Returns length of each piece in bytes
        """
        return self.meta_info[b"info"][b"piece length"]

    @property
    def total_length(self) -> int:
        """
        Returns the total size of files in bytes
        """
        if self.is_multi_file:
            raise RuntimeError("Torrent contains multiple files. This is not supported currently.")
        return self.files[0].length

    @property
    def pieces(self):
        """
        Returns a list containing the SHA1 (each 20 bytes long) of all the pieces
        """
        data = self.meta_info[b"info"][b"pieces"]
        pieces, offset, length = [], 0, len(data)
        pieces = [data[offset: offset + 20] for offset in range(0, length, 20)]
        return pieces

    @property
    def output_file(self):
        logging.info("Torrent output file: {0}".format(self.meta_info[b"info"][b"name"].decode("utf-8")))
        return self.meta_info[b"info"][b"name"].decode("utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Torrent's absolute file path", type=str, required=True)
    args = parser.parse_args()

    t = Torrent(torrent_path=args.file)
    tr = Tracker(t)

    loop = asyncio.get_event_loop()
    r1 = loop.run_until_complete(tr.connect())
    print(r1)
