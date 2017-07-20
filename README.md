# Chord Summary
Chord is lookup and maintenance protocol for storing key-value pairs in a distributed hash table. The chord protocol specifies how to join new peers to an existing network, how to map hashed keys onto peers, how to recover from peer departures and failures, and how to replicate data over the network. As a result, the chord protocol defines an abstract storage medium that is decentralized, load balanced, fault tolerant, and available.

## Project Description
A distributed hash table designed from various research papers on the Chord protocol. This JavaScript implementation runs on Node.js and relies on gRPC protocol buffers to communicate with peers. Demo applications include a command line interface for accessing hash table contents and a peer-to-peer (p2p) messenger. Further development will include encrypted message-passing and a full rewrite in a compiled language. Future applications of this module will be for an encrypted p2p messenger.

## API

join(address)

get(key)

set(key, value)

del(key)