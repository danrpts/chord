### Chord Protocol

- [x] Bucket class
- [ ] Bucket replication over successor list
- [x] Bucket partition on join
- [x] Bug fix: partition before set and set before notify
* [x] Bug fix: invalid partitioning due to improper use of isBetween
  - known edge case of isBetween utility since it's made for identifier circle range checking
  - consider defining utility methods for other special identifier circle range check
  - added isStrictlyBetween utility for the partition method (moveKeys in some literature)
- [ ] Optimize lookup procedure using finger table and successor list
- [ ] Complete linear and lg route generators and integrate with lookup procedure 
- [x] successor and predecessor up/down events
- [x] Is fixPredecessor really necessary if notify is running per fixSuccessor? Removing it for now.
* [x] Find unhandled promise rejection when node fails
  - was an artifact of the temporary lookup procedure throwing when the immediate successor is unavailable
* [x] Find and patch (internal type) memory leak
  - grpc update with new client.close() method
  - https://github.com/grpc/grpc/commit/ffac55dfd634a62d54c8f152bbbce2bb51e7bc31#diff-e5ada239817cf2b845febcf5c7be68f0
  - https://www.alexkras.com/simple-guide-to-finding-a-javascript-memory-leak-in-node-js/
- [ ] Remove await in loop lib/chord.js
- [ ] Double check partitioning bounds logic
- [ ] Remove get-port dependency in lib files
- [x] Remove IP dependency in lib files
- [ ] JSDoc
- [ ] Comments
- [ ] Unit tests
- [x] Bind random port when 0
- [ ] Status codes
- [ ] Optional WebSocket support
- [ ] Optional finger table
- [ ] Optional dynamic successor list (r length list requires r+1 peers in network)
