'use strict';

const _Peer = require('./Peer.js');
const Peer = _Peer.Peer;
const rpc = _Peer.rpc;

/**
 * HIGH-LEVEL CHORD CONSTANTS
 *
 */
//const A = 'sha1'; // default identifier space hashing function
const M = 4; // default finger-list length (should be 160 for sha1)
const R = 1; // default successor-list-length and replica-set-size

/**
 * HIGH-LEVEL CHORD API
 *
 */

/**
 *
 */
function createPeer (port, nFingers, nSuccessors) {

  //algorithm = algorithm ? algorithm : A;

  nFingers = (nFingers > 0) ? nFingers : M;

  nSuccessors = (nSuccessors > 0) ? nSuccessors : R;

  return new Peer(port, nFingers, nSuccessors);

}

/**
 *
 */
async function ping (node, address) {

  if (!Peer.isJoined(node)) {
    throw new Error('"node" argument is not joined to network');
  }

  if (!Peer.isAddress(address)) {
    throw new Error('"address" argument must be compact IP-address:port');
  }

  return rpc(address, 'ping', { address: node.address });

}

/**
 *
 */
async function echo (node, address, message) {

  if (!Peer.isJoined(node)) {
    throw new Error('"node" argument is not joined to network');
  }

  if (!Peer.isAddress(address)) {
    return new Error('"address" argument must be compact IP-address:port');
  }

  return rpc(address, 'echo', { message, address: node.address });

}

/**
 *
 */
async function join (node, address) {

  if (Peer.isJoined(node)) {
    throw new Error('"node" argument must not be joined');
  }

  if (!Peer.isAddress(address)) {
    return new Error('"address" argument must be compact IP-address:port');
  }

  // bootstrap steps setup successor list (do not wait for stabilize to do these steps)
  try {

    // get this node's successor and call it s
    let s = await rpc(address, 'findSuccessor', { id: node.id });

    // s is live successor
    //node.replicateFrom(s.address);

    // get s's predecessor and call is p
    let p = await rpc(s.address, 'getPredecessor');

    /* invariant: p is this node's predecessor */

    // get p's successor list and call it l
    let l = await rpc(p.address, 'getSuccessorList');

    // set p as predecessor
    node.predecessor = p.address;

    // borrow p's list l (at most k)
    for (let i = 0; i < node.k; i++) {
      
      // l has fewer elements (at least 1)
      if (l.addrs.length <= i)  {
      
        // * NOTE 
        node.successorList[i] = node.address;
      
      } else {

        node.successorList[i] = l.addrs[i];

      }

    }

    // NOTE
    // - do not emit successor change because we replicated from it

  } catch (e) {
  
    throw e;

  }

}

/**
 *
 */
async function leave (node) {

  if (!Peer.isJoined(node)) {
    throw new Error('"node" argument must be joined');
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
 *
 */
async function get (node, key) {

  if (!Peer.isJoined(node)) {
    throw new Error('"node" argument must be joined');
  }

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  var id = Peer.toSha1(key);

  try {

    let s = await findSuccessor.call(node, id);

    return rpc(s.address, 'get', { id });

  } catch (e) {
  
    throw e;

  }

}

/**
 *
 */
async function set (node, key, value) {

  if (!Peer.isJoined(node)) {
    throw new Error('"node" argument must be joined');
  }

  if (!_.isString(key)) {
    throw new Error('"key" argument must be string');
  }

  if (!Buffer.isBuffer(value)) {
    throw new Error('"value" argument must be buffer');
  } 

  var id = Peer.toSha1(key);

  try {

    let s = await findSuccessor.call(node, id);

    return rpc(s.address, 'set', { id, value });

  } catch (e) {
  
    throw e;

  }

}

/**
 *
 */
async function del (node, key) {

  try {

    if (!Peer.isPeer(node) && !Peer.isJoined(node)) {
      throw new Error('"node" argument must be instance of Peer and joined');
    }

    if (!_.isString(key)) {
      throw new Error('"key" argument must be string');

    }

    let id = Peer.toSha1(key);

    let s = await findSuccessor.call(node, id);

    return rpc(s.address, 'delete', { id });

  } catch (error) {
  
    console.error("delete", error);

  }

}

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