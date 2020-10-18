# Pears: A simple BitTorrent client

[![forthebadge](https://forthebadge.com/images/badges/made-with-python.svg)](https://forthebadge.com)
[![forthebadge](https://forthebadge.com/images/badges/built-with-love.svg)](https://forthebadge.com)
<br><br>
[![Generic badge](https://img.shields.io/badge/python-3.7.6-success.svg)](https://www.python.org/downloads/release/python-376/)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-no-red.svg)](https://bitbucket.org/lbesson/ansi-colors)
[![stability-experimental](https://img.shields.io/badge/stability-experimental-orange.svg)](https://github.com/emersion/stability-badges#experimental)
[![GitHub license](https://img.shields.io/github/license/Naereen/StrapDown.js.svg)](https://opensource.org/licenses/MIT)

This is a very simple BitTorrent client that I implemented for fun and to learn more about P2P protocols like BitTorrent. On the way, I learned the following:
1. Python's `asyncio` library for asyncronous programming
2. How to structure a big project
<br>

Limitations - 
1. Only supports downloading a torrent with single file
2. Doesn't support seeding

## How to run
```
$ pip install -r requirements.txt

$ python cli.py --file C:\Users\chaitanya\Documents\torrents\linuxmint-18-cinnamon-64bit.iso.torrent
```

## How it works
The main controller is the `TorrentClient`. It is responsible for the following:
* Connecting to `Tracker` and getting the peer information. Created a Queue with peers that workers will consume from.
* Starting workers to consume data from the peers
* Using `PieceManager` to manage the torrent pieces
  - `PieceManager` is also responsible for devising a stratedy as to which piece to be requested next for a peer

`Protocol` implementes the BitTorrent protocol. The unofficial specification can be found here: https://wiki.theory.org/BitTorrentSpecification
<br>

## Future work
1. Support torrents with multiple files
2. Support seeding aka uploading data to other peers
3. Improve performance

Read more about each module in `/docs`
## License
The client is released under the MIT license, see LICENSE.
