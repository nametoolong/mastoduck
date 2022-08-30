# Mastoduck

> If it looks like a mastodon, swims like a mastodon and toots like a mastodon, then it probably is a mastodon.

Mastoduck is a reimplementation of Mastodon's streaming server in [vibe.d](https://vibed.org/). You need [a modified version of Mastodon](https://github.com/nametoolong/mastodon) to use this module.

Mastoduck is not very stable (yet). Use at your own risk!

## Installation

A working D compiler is required to compile the project. DMD is assumed to be installed in this section. If you are building using other compilers, expect to tweak a handful of settings.

First, you need to add the `ddb` submodule as a local dependency. Note this may interfere with other projects that depend on the official version of `ddb`. In case any other D project went wrong, try removing this local dependency first.
```bash
dub add-local ddb
```

Then compile with the appropriate flags.
```bash
dub build -b debug # build the debug version
dub -b unittest # run the unit tests
DFLAGS=-check=bounds=on dub build -b release # build the release version, with bounds checking enabled to mitigate safety issues
```

Mastoduck does not support PostgreSQL's Unix domain sockets and SCRAM-SHA1 authentication method. You may need to enable MD5 authentication, reset the database users and change `DB_HOST` to `127.0.0.1` before running Mastoduck.

The compiled executable can be run from anywhere as long as the appropriate `.env` or `.env.production` file is present. It reads the `RAILS_ENV` environment variable and configures correspondingly. Ideally, this should work given you followed the official guide when installing Mastodon.
```bash
# Be sure to stop the Node.js streaming server first!
cd /home/mastodon/live
RAILS_ENV=development mastoduck/mastoduck
```

To run it in an production environment, simply set `RAILS_ENV` to `production`. Alternatively, you can manually specify the environment variables:
```bash
env RAILS_ENV=development PORT=4000 BIND=0.0.0.0 DB_HOST=127.0.0.1 DB_PORT=5432 DB_USER=mastodon DB_PASS=123456 ./mastoduck
```

A systemd unit file is provided in the Mastodon fork. However, making the server run smoothly might require more tinkering with the settings.