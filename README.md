We have the hovercraft. It is fast and it skims just above the surface.

There is much to be added to the hovercraft, but it already flies.

## Welcome to Hovercraft

An easy direct Erlang CouchDB library.

Use this to abstract CouchDB behind a simple Erlang function call. Hovercraft currently
supports the database and document APIs, with views on the way.

## Basic Usage

NOTE: Hovercraft is only compatible with CouchDB trunk. If the tests fail,
make sure you are running on the latest CouchDB.

The easiest way to try Hovercraft is to link the hovercraft directory
inside the top level of your CouchDB trunk directory.  Assuming the hovercraft
source is cloned within the same parent dir as the CouchDB source:

    cd couchdb
    ln -s ../hovercraft .

You may need to modify the hovercraft.erl file so that the include path
points to the copy of 'couch_db.hrl' in your local CouchDB source dir.
The default should work if you linked hovercraft into the root of
the CouchDB src dir as shown above.  If not you can modify it to be
something like:

    %%-include("src/couchdb/couch_db.hrl").
    -include("/Users/YOURNAME/src/git/couchdb/src/couchdb/couch_db.hrl").

Finally compile hovercraft and launch CouchDB in interactive mode. If your
compile runs clean you should have a new hovercraft.beam file in the root
of your CouchDB src dir.

    erlc hovercraft/*erl && make dev && utils/run -i

This will open an interactive session. To run the tests, call
hovercraft:test/0 like this:

    1> hovercraft:test().
    [info] [<0.30.0>] Starting tests in <<"hovercraft-test">>
    ok

## Speed of Light

To run the speed of light test, run hovercraft:lightning/0 like this (It defaults to batch size of '1000'):

    2> hovercraft:lightning().
    Inserted 100000 docs in 20.638469 seconds with batch size of 1000. (4845.3206485422925 docs/sec)
    ok

To try different tunings, you can call hovercraft:lightning/1 with
custom batch sizes:

    3> hovercraft:lightning(100).
    Inserted 100000 docs in 22.853769 seconds with batch size of 100. (4375.645872678594 docs/sec)
    ok

The docs in the speed of light test are small, feel free to edit
the source code to try larger docs.

## Credits

Released at #CouchHack '09
Apache 2.0 License
Copyright 2009 J. Chris Anderson <jchris@couch.io>
