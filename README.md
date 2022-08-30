# Mastoduck

> If it looks like a mastodon, swims like a mastodon and toots like a mastodon, then it probably is a mastodon.

A reimplementation of Mastodon's streaming module in vibe.d. You need [a modified version of Mastodon](https://github.com/nametoolong/mastodon) to use this module. Use at your own risk!

## Usage

A working D compiler is required to compile the project.

First, add the `ddb` submodule as a local dependency.
```
dub add-local ddb
```
Then compile with appropriate arguments.
```
dub build -b debug # build the debug version
DFLAGS=-check=bounds=on dub build -b release # build the release version, with bounds checking enabled to mitigate safety issues
dub -b unittest # run the unittest
```
