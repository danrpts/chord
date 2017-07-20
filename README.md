## Chord Summary
Chord is lookup and maintenance protocol for storing key-value pairs in a distributed hash table. The chord protocol specifies how to join new peers to an existing network, how to map hashed keys onto peers, how to recover from peer departures and failures, and how to replicate data over the network. As a result, the chord protocol defines an abstract storage medium that is decentralized, load balanced, fault tolerant, and available.

## Project Description
This JavaScript implementation of the chord protocol is designed to run on Node.js, and is inspired from many academic papers written on the topic of chord. The implementation also makes heavy use of async/await (Node >=7.6) and generator functions, and relies on gRPC protocol buffers to simplify peer message-passing during maintenance procedures. Demo applications, which may be found in the ./examples directory, include a tool for interfacing with a chord peer/network, and a simple peer-to-peer chat program. Further development of this project may include the use of public keys as node identifiers, encrypted message-passing, UDP transport layer, and a full rewrite in some compiled language.

## API

join(address)

get(key)

set(key, value)

del(key)