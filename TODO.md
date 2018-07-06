### Overall

- [ ] LAN discovery for bootstraping
- [ ] Plain and encrypted channels (DH key exchange)
- [ ] NAT Traveral: UPnP
- [ ] Nodes should perform a handshake, where they confirm versions and capabilities, and then optionally upgrade to an encrypted channel using a DH key exchange
- [ ] Node IDs should be public keys, and each node should maintain a keychain for signing messages

### Maintenance Protocol

- [ ] Non-overlapping calls
- [ ] Use minimum/maximum period between calls

### Chord class

- [x] bind random port when 0
- [x] fixPrecessor removal using pure notify
- [x] successor and predecessor up/down events
- [ ] linear route generator using successor list
- [ ] lg route generator using finger table and successor list
- [ ] fix await in loop of fixSuccessor
- [ ] handle IP address change

### Bucket class

- [ ] successor list replication
- [ ] updates on replicas upon set event
- [x] partitioning upon fully joined event
- [ ] double check partitioning bounds logic
- [ ] use some type of data store for hash table contents

### Misc

- [ ] JSDoc
- [x] ESlint
- [ ] comments
- [ ] unit tests
- [ ] documentation
- [ ] RPC protocol review
- [ ] define and use status codes
- [ ] Remove get-port dependency in lib files
- [x] Remove IP dependency in lib files

### Bugs

- [ ] there might be a bug in the lookup server side procedure that throws invalid successor state error
- [x] Bug fix: incorrect FINGER_BASE initialization
- [x] Bug fix: partition before set and set before notify

* [x] Bug fix: invalid partitioning due to improper use of isBetween
  - known edge case of isBetween utility since it's made for identifier circle range checking
  - consider defining utility methods for other special identifier circle range check
  - added isStrictlyBetween utility for the partition method (moveKeys in some literature)
* [x] Memory leak: internal type
  - gRPC update
  - new client.close() method https://github.com/grpc/grpc/commit/ffac55dfd634a62d54c8f152bbbce2bb51e7bc31#diff-e5ada239817cf2b845febcf5c7be68f0
  - https://www.alexkras.com/simple-guide-to-finding-a-javascript-memory-leak-in-node-js/
* [x] Unhandled promise rejection when node fails
  - artifact of temporary lookup procedure throwing when immediate successor is unavailable
