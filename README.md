## Chord Summary
Chord is lookup and maintenance protocol for storing key-value pairs in a distributed hash table (DHT). The chord protocol specifies how to join new peers to an existing network, how to map hashed-keys onto peers, how to recover from peer departures and failures, and how to replicate data over the network. As a result, the chord protocol defines an abstract storage medium that is decentralized, load balanced, fault tolerant, and available.

## Project Details
This JavaScript implementation of the chord protocol is designed to run on Node.js, and inspired from many academic papers written on the topic of chord. The chord protocol does not define how key-value pairs are to be stored and accessed within the DHT as this is trivial with a data store of choice and the specified lookup procedure. Thus, this project is split into two main class files (i.e. ./lib/chord.js and ./lib/bucket.js). The chord class exposes an API for creating peers and looking up keys, while the bucket class exposes an API for actually getting, setting, and deleting values withing the DHT. This implementation also makes heavy use of async/await (so Node version >=7.6 is required) and generator functions; it also relies on gRPC protocol buffers to simplify peer message-passing during maintenance procedures which differs from the traditional RPC over UDP used in the chord specification. Demo applications using the class files may be found in the ./examples directory, which includes a tool for interfacing with a chord peer or network, and a simple peer-to-peer chat tool. Further development of this project may use of public keys as node identifiers, encrypted message-passing, a UDP transport layer, and a full rewrite in some compiled language.

## Installation
`git clone https://github.com/danrpts/chord.git`
`cd ./chord`
`npm install`

## Using example applications
To spin up a peer on a random port:
`npm run cli -- -p 0`
Now to view CLI commands:
`help`
or
To spin up a peer on a random port, join a network, and set you chat nickname use the following command:
`npm run chat -- --join=<host> --nick=<string>`
Now to send a hello world message to another user:
`/tell <string> hello world!`

## Chord API

ping(host)

state(host)

join(host)

lookup(key)

## Bucket API

get(key)

has(key)

set(key, value)

del(key)

dump(host)