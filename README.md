We have the hovercraft. It is fast and it skims just above the surface.

There is much to be added to the hovercraft, but it already flies.

## Welcome to Hovercraft

An easy direct Erlang CouchDB library.

Use this to abstract CouchDB behind a simple Erlang function call. Currently supports the database and document APIs, with views on the way.

## Basic Usage

NOTE: Hovercraft is only compatible with CouchDB trunk. If the tests fail,
make sure you are running on the latest CouchDB.

You will likely need to modify the hovercraft.erl file so that it
the include path points to the copy of 'couch_db.hrl' in your local
CouchDB source dir.

e.g.

    %%-include("couch_db.hrl").
    -include("/Users/YOURNAME/src/git/couchdb/src/couchdb/couch_db.hrl").

The easiest way to try Hovercraft is to link the hovercraft directory
inside the top level of your CouchDB trunk directory and then
launch CouchDB in interactive mode:

    cd couchdb
    ln -s ../hovercraft .
    erlc hovercraft/*erl && make dev && utils/run -i

This will open an interactive session. To run the tests, call
hovercraft:test/0 like this:

    1> hovercraft:test().
    [info] [<0.30.0>] Starting tests in <<"hovercraft-test">>
    ok

## Speed of Light

To run the speed of light test, run hovercraft:lightning/0 like this:

    2> hovercraft:lightning().
    Inserted 100000 docs in 14.967256 seconds with batch size of 1000. (6681.251393040915 docs/sec)
    ok

To try different tunings, you can call hovercraft:lightning/1 with
custom batch sizes. The docs in the speed of light test are small, feel
free to edit the source code to try larger docs.

## Credits

Released at #CouchHack '09
Apache 2.0 License
Copyright 2009 J. Chris Anderson <jchris@couch.io>
