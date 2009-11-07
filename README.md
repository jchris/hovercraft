# Hovercraft

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

You may need to modify the hovercraft.erl and hovercraft_test.erl files so that the
include path points to the copy of 'couch_db.hrl' in your local CouchDB source dir.
The default should work if you linked hovercraft into the root of
the CouchDB src dir as suggested above.  If not you can modify it to be
something like:

    %%-include("src/couchdb/couch_db.hrl").
    -include("/Users/YOURNAME/src/git/couchdb/src/couchdb/couch_db.hrl").

Finally you can compile hovercraft and launch CouchDB in interactive mode. If your
compile runs clean you should have new hovercraft.beam and hovercraft_test.beam files
in the root of your CouchDB src dir.

    erlc hovercraft/*erl && make dev && utils/run -i

This will open an interactive erl shell where you can call hovercraft functions directly.

## Running Tests

Hovercraft comes with its own test suite which you can run to verify that everything is
working properly.  The following test functions are exported:

    hovercraft_test:all/0
    hovercraft_test:all/1
    hovercraft_test:chain/0
    hovercraft_test:lightning/0
    hovercraft_test:lightning/1

To run the complete test suite try:

    1> hovercraft_test:all().
    [info] [<0.30.0>] Starting tests in <<"hovercraft-test">>
    ok

### Testing the Speed of Light

The test suite includes the 'lightning' tests which exercise CouchDB at its maximum speed.

To run the speed of light test, run hovercraft_test:lightning/0 like this (It defaults to batch size of '1000'):

    2> hovercraft_test:lightning().
    Inserted 100000 docs in 20.638469 seconds with batch size of 1000. (4845.3206485422925 docs/sec)
    ok

To try different tunings, you can call hovercraft_test:lightning/1 with
custom batch sizes:

    3> hovercraft_test:lightning(100).
    Inserted 100000 docs in 22.853769 seconds with batch size of 100. (4375.645872678594 docs/sec)
    ok

The docs in the speed of light test are small, feel free to edit the source code to try larger docs.

## TODO

Hovercraft is currently only partially functional with the current trunk (0.11.x).  The following
issues (and probably more!) currently need to be resolved (patches welcome!):

hovercraft_test:chain/0

    hovercraft_test:chain/0 calls hovercraft:query_view/5 and throws the following exception:

    2> hovercraft_test:chain().
    [info] [<0.131.0>] checkpointing view update at seq 2 for chain-test _design/view-test
    [info] [<0.131.0>] checkpointing view update at seq 180 for chain-test _design/view-test
    [info] [<0.131.0>] checkpointing view update at seq 201 for chain-test _design/view-test
    ** exception error: undefined function couch_view:fold_reduce/7
         in function  hovercraft:query_view/5
         in call from hovercraft_test:do_chain/4


hovercraft_test:all/0

    The call (currently commented out) to 'should_stream_attachment(DbName)' fails with the error:

    1> hovercraft_test:all().
		[info] [<0.31.0>] Starting tests in <<"hovercraft-test">>
		[error] [emulator] Error in process <0.128.0> with exit value: {{nocatch,{not_found,"Document is missing attachment"}},[{hovercraft,attachment_streamer,3}]}



		=ERROR REPORT==== 7-Nov-2009::14:02:55 ===
		Error in process <0.128.0> with exit value: {{nocatch,{not_found,"Document is missing attachment"}},[{hovercraft,attachment_streamer,3}]}

		** exception exit: {nocatch,{not_found,"Document is missing attachment"}}
		     in function  hovercraft:attachment_streamer/3

hovercraft_test:all/0

    The call (currently commented out) to 'should_query_views(DbName)' fails with the error:

    1> hovercraft_test:all().
		[info] [<0.31.0>] Starting tests in <<"hovercraft-test">>
		[error] [emulator] Error in process <0.122.0> with exit value: {{nocatch,{not_found,"Document is missing attachment"}},[{hovercraft,attachment_streamer,3}]}



		=ERROR REPORT==== 7-Nov-2009::14:05:10 ===
		Error in process <0.122.0> with exit value: {{nocatch,{not_found,"Document is missing attachment"}},[{hovercraft,attachment_streamer,3}]}

		** exception exit: {nocatch,{not_found,"Document is missing attachment"}}
		     in function  hovercraft:attachment_streamer/3


## Credits

Released at #CouchHack '09

Apache 2.0 License

Copyright 2009 J. Chris Anderson <jchris@couch.io>

