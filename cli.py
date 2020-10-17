import argparse
import asyncio
import signal

from logger import init_logger
from torrent import Torrent
from client import TorrentClient

if __name__ == "__main__":
    logging = init_logger(__name__, testing_mode=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="Torrent's absolute file path", type=str, required=True)
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose output', required=False)
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
        loop.run_until_complete(task)
    except Exception as e:
        logging.exception(e)
