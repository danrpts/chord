'use strict';
const _ = require('underscore');
const peer = require('./dht_peer.js');
const Peer = peer.Peer;
const rpc = peer.rpc;

/**
 * DEFINE HIGH-LEVEL CHORD PEER CONSTANTS
 *
 */
const M = 4; // default finger-list length (should be 160 for sha1)
const R = 1; // default successor-list-length and replica-set-size

/**
 * DEFINE HIGH-LEVEL CHORD CLIENT API
 *
 */

/**
 * Create a new node
 */
function createPeer (port, nFingers, nSuccessors) {

  //algorithm = algorithm ? algorithm : A;

  nFingers = (nFingers > 0) ? nFingers : M;

  nSuccessors = (nSuccessors > 0) ? nSuccessors : R;

  return new Peer(port, nFingers, nSuccessors);

}

/**
 * Ping a receiving node from this node
 */
async function ping (self, receiver) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  if (!Peer.isAddress(receiver)) {
    throw new Error('"receiver" argument must be compact IP-address:port');
  }

  var request = { sender: self.address };

  return rpc(receiver, 'ping', request);

}

/**
 * Echo a message from this node onto a receiving node
 */
async function echo (self, receiver, message) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  if (!Peer.isAddress(receiver)) {
    return new Error('"receiver" argument must be compact IP-address:port');
  }

  var request = { message, sender: self.address };

  return rpc(receiver, 'echo', request);

}

/**
 * 
 * NOTE: Only replicates entries that belong to this node
 */
async function partition (self, receiver) {

  // invalid state
  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  var predecessorAddress = self.predecessor;

  // invalid predecessor state
  if (!Peer.isAddress(predecessorAddress)) {
    return cb(new Error());
  }

  // invalid request
  if (!Peer.isAddress(receiver)) {
    throw new Error('"receiver" argument must be compact IP-address:port');
  }

  // invalid request
  if (receiver === self.address) {
    throw new Error('cannot replicate from self');
  }

  var partitionRequest = { lower: Peer.toSha1(predecessorAddress), upper: self.id };

  try {

    // request owned bucket entries
    let partitionResponse = await rpc(receiver, 'partition', partitionRequest);

    // copy entries into bucket
    for (let entry of partitionResponse.entries) {

      let key = entry.id.toString('hex');

      self.bucket[key] = entry.value;

    }

    return partitionResponse;

  } catch (e) {

    throw e;

  }

}

/**
 * 
 * NOTE: Only replicates entries that belong to receiving node
 */
async function merge (self, receiver) {

  // invalid state
  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  // invalid request
  if (!Peer.isAddress(receiver)) {
    throw new Error('"receiver" argument must be compact IP-address:port');
  }

  // invalid request
  if (receiver === self.address) {
    throw new Error('cannot replicate to self');
  }

  var mergeRequest = { entries: [] };

  for (let key in self.bucket) {
    
    let keyId = Buffer.from(key, 'hex');
    
    let value = self.bucket[key];
    
    mergeRequest.entries.push({ id: keyId, value });

  }

  try {

    return await rpc(receiver, 'merge', mergeRequest);

  } catch (e) {

    throw e;

  }

}

/**
 * Join this node to a DHT network via a receiving node
 */
async function join (self, receiver) {

  // invalid state
  if (Peer.isJoined(self)) {
    throw new Error('"node" argument must not be joined');
  }

  // invalid state, wait 2 seconds for notify
  if (!Peer.isAddress(this.predecessor)) {
    //setTimeout(() => {}, 2000);
  }

  // invalid request
  if (!Peer.isAddress(receiver)) {
    return new Error('"address" argument must be compact IP-address:port');
  }

  // invalid request
  if (receiver === self.address) {
    throw new Error('cannot join to self');
  }  

  var findSuccessorRequest = { id: self.id };

  // bootstrap steps setup successor list (do not wait for stabilize to do these steps)
  try {

    // get this node's successor and call it s
    let findSuccessorResponse = await rpc(receiver, 'findSuccessor', findSuccessorRequest);

    // get s's predecessor and call is p
    let getPredecessorResponse = await rpc(findSuccessorResponse.address, 'getPredecessor');

    /* invariant: p is this node's predecessor */
    self.predecessor = getPredecessorResponse.address;

    // get p's successor list and call it l
    let getSuccessorListResponse = await rpc(self.predecessor, 'getSuccessorList');

    // TODO
    // - handle unequal k values (received list is different size than this list)
    self.successorList = getSuccessorListResponse.addresses.slice(0, self.k);

    // perform a notify so replicate can occur with ease
    let notifierRequest = { sender: self.address };

    // notify new successor about this node (refresh its predecessor)
    await rpc(self.successorList[0], 'notify', notifierRequest);

    // receive partition from successor
    await partition(self, self.successorList[0]);

    // NOTE
    // - it should partition from successor
    // - it should partition from predecessor

    // NOTE
    // - do not emit successor change because we replicated from it

  } catch (e) {
  
    throw e;

  }

}

/**
 * Leave the DHT network this node belongs to
 */
async function leave (self) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  try {

    // TODO
    // - get live successor
    //await this.replicateTo(this.successorList[0]);
    //let successor = await nextLiveSuccesor.call(node);

    // reset predecessor
    //node.predecessor = null;

    // reset successor list
    //node.successorList.fill('');

    // reset immediate successor
    //node.successorList[0] = node.address;

    // TODO
    // - first on self: send s = this.successorList[this.k - 1] to predecessor
    // - then on predecessor: remove s from successor list
    // - then on predecessor: push (last successor) to successor list
    
    // TODO
    // - first on self: send p = this.predecessor to successor
    // - then on successor: replace predecessor with p


  } catch (e) {

    throw e;

  }

}

/**
 * Get a value from the DHT network via this node and key
 */
async function get (self, key) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  var id = Peer.toSha1(key);

  var findSuccessorRequest = { id };

  try {

    let findSuccessorResponse = await rpc(self.address, 'findSuccessor',  findSuccessorRequest);

    return rpc(findSuccessorResponse.address, 'get', { id });

  } catch (e) {
  
    throw e;

  }

}

/**
 * Set a value to the DHT network via this node and key
 */
async function set (self, key, value) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  if (!Buffer.isBuffer(value)) {
    throw new Error('"value" argument must be buffer');
  } 

  var id = Peer.toSha1(key);

  var findSuccessorRequest = { id };

  try {

    let findSuccessorResponse = await rpc(self.address, 'findSuccessor',  findSuccessorRequest);

    return rpc(findSuccessorResponse.address, 'set', { id, value });

  } catch (e) {
  
    throw e;

  }

}

/**
 * Delete a value in the DHT network via this node and key
 */
async function del (self, key) {

  if (!Peer.isPeer(self) && !Peer.isJoined(self)) {
    throw new Error('"self" argument must be instance of Peer and joined');
  }

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');

  }

  var id = Peer.toSha1(key);

  var findSuccessorRequest = { id };

  try {

    let findSuccessorResponse = await rpc(self.address, 'findSuccessor',  findSuccessorRequest);

    return rpc(findSuccessorResponse.address, 'delete', { id });

  } catch (e) {
  
    throw e

  }

}

/**
 * EXPOSE HIGH-LEVEL CHORD CLIENT API
 *
 */
module.exports = {
  Peer,
  createPeer,
  ping,
  echo,
  partition,
  merge,
  join,
  leave,
  get,
  set,
  delete: del
}
