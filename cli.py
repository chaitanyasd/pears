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

import argparse
import asyncio
import signal

from client import TorrentClient
from logger import init_logger, debug_logging_enabled
from torrent import Torrent

logging = init_logger(__name__, testing_mode=debug_logging_enabled)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Torrent's absolute file path", type=str, required=True)
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    client = TorrentClient(Torrent(torrent_path=args.file))
    task = asyncio.ensure_future(client.start())


    def signal_handler(*_):
        logging.warning('Exiting, please wait until everything is shutdown...')
        client.stop()
        task.cancel()


    signal.signal(signal.SIGINT, signal_handler)

    try:
        logging.info("Starting client task")
        loop.run_until_complete(task)
    except Exception as e:
        logging.exception(e)
