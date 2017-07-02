'use strict';
const _ = require('underscore');
const Peer = require('./Peer.js');
const util = require('./util.js');
const isPort = util.isPort;
const isAddress = util.isAddress;
const isBetween = util.isBetween;
const toSha1 = util.toSha1;
const doRpc = util.doRpc;

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

  if (!isAddress(receiver)) {
    throw new Error('"receiver" argument must be compact IP-address:port');
  }

  var pingRequest = {
    sender: {
      ipv4: self.address
    }
  };

  return doRpc(receiver, 'ping', pingRequest);

}

/**
 * Echo a message from this node onto a receiving node
 */
async function echo (self, receiver, message) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  if (!isAddress(receiver)) {
    return new Error('"receiver" argument must be compact IP-address:port');
  }

  var echoRequest = { 
    message, 
    sender: {
      ipv4: self.address
    }
  };

  return doRpc(receiver, 'echo', echoRequest);

}

/**
 * Join this node to a DHT network via a receiving node
 */
async function join (self, receiver) {

  // invalid state
  if (Peer.isJoined(self)) {
    throw new Error('"node" argument must not be joined');
  }

  // invalid request
  if (!isAddress(receiver)) {
    return new Error('"address" argument must be compact IP-address:port');
  }

  // invalid request
  if (receiver === self.address) {
    throw new Error('cannot join to self');
  }

  try {

    let lookupRequest = {
      id: toSha1(self.address)
    };
 
    let lookupResponse = await doRpc(receiver, 'lookup', lookupRequest);

    let newSuccessor0 = lookupResponse.successor.ipv4;

    // invalid response
    if (!isAddress(newSuccessor0)) {
      throw new Error();
    }

    let infoResponse = await doRpc(newSuccessor0, 'info');

    // predecessor address
    let newPredecessor = infoResponse.predecessor.ipv4;

    // invalid response
    if (!isAddress(newPredecessor)) {
      throw new Error();
    }

    // // get successor list
    let updatedSuccessor = _.pluck(infoResponse.successor, 'ipv4');

    // TODO
    // - correctly handle non-equal r values
    // remove last successor
    updatedSuccessor = updatedSuccessor.slice(0, self.r - 1);

    // // prepend successor
    updatedSuccessor.unshift(newSuccessor0);

    self.successor = updatedSuccessor;

    //self.emit('successor::up', updatedSuccessor);

    let notifyRequest = {
      sender: {
        ipv4: self.address
      }
    };

    // notify new successor about this node (refresh its predecessor)
    let notifyResponse = await doRpc(newSuccessor0, 'notify', notifyRequest);

    // TODOs
    // - check response status
    // split partition from new successor
    //let splitResponse = await split(self, newSuccessorAddress0);

  } catch (e) {

    throw e;

  }

  // TODOs
  // return status

}

/**
 * Leave the DHT network this node belongs to
 */
async function leave (self) {

  if (!Peer.isJoined(self)) {
    throw new Error('"self" argument must be joined');
  }

  try {

    // merge partition with predecessor
    //await merge(self, self.predecessor);

    // merge partition with successor
    //await merge(self, self.successor[0]);

    // reset predecessor
    self.predecessor = null;

    // reset successor list
    self.successor.fill('');

    // reset immediate successor
    self.successor[0] = self.address;

    // TODOs
    // - first on self: send s = this.successorList[this.k - 1] to predecessor
    // - then on predecessor: remove s from successor list
    // - then on predecessor: push (last successor) to successor list
    
    // TODOs
    // - first on self: send p = this.predecessor to successor
    // - then on successor: replace predecessor with p

    clearInterval(self.timeout);
    
    self.server.tryShutdown(()=>{});

  } catch (e) {

    throw e;

  }

}

/**
 * Get a value from the DHT network via this node and key
 */
async function get (self, key) {

  //if (!Peer.isJoined(self)) {
  //  throw new Error('"self" argument must be joined');
  //}

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  var id = toSha1(key);

  var lookupRequest = { id };

  try {

    let lookupResponse = await doRpc(self.address, 'lookup', lookupRequest);

    let successor0 = lookupResponse.successor.ipv4;

    // invalid response
    if (!isAddress(successor0)) {
      throw new Error();
    }

    return doRpc(successor0, 'get', { id });

  } catch (e) {
  
    throw e;

  }

}

/**
 * Set a value to the DHT network via this node and key
 */
async function set (self, key, value) {

  //if (!Peer.isJoined(self)) {
  //  throw new Error('"self" argument must be joined');
  //}

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  if (!Buffer.isBuffer(value)) {
    throw new Error('"value" argument must be buffer');
  } 

  var id = toSha1(key);

  var lookupRequest = { id };

  try {

    let lookupResponse = await doRpc(self.address, 'lookup', lookupRequest);

    let successor0 = lookupResponse.successor.ipv4;

    // invalid response
    if (!isAddress(successor0)) {
      throw new Error();
    }

    return doRpc(successor0, 'set', { id, value });

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

  var id = toSha1(key);

  var lookupRequest = { id };

  try {

    let lookupResponse = await doRpc(self.address, 'lookup', lookupRequest);

    let successor0 = lookupResponse.successor.ipv4;

    // invalid response
    if (!isAddress(successor0)) {
      throw new Error();
    }

    return doRpc(successor0, 'delete', { id });

  } catch (e) {
  
    throw e;

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
  join,
  leave,
  get,
  set,
  delete: del
}
