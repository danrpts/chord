'use strict';
const _ = require('underscore');
const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const bignum = require('bignum');
const Buffer = require('buffer').Buffer;
const EventEmitter = require('events').EventEmitter;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

// 160 because using SHA1 hash function
const MAX_ID = bignum.pow(2, 160);

// precomputed powers of 2
var fingerBase;

// TODO
// - return own status codes
// - handle gRPC errors
const STATUS_CODES = {};

/*** BEGIN PEER CLASS ***/
/*** BEGIN PEER CLASS ***/
/*** BEGIN PEER CLASS ***/

/**
 *
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port, m, r) {

    super();

    // TODO
    // - use random port when port is 0

    if (!Peer.isPort(port)) {
      throw new Error('"port" argument must be number between 1 and 65536');
    }

    // set address
    this.address = ip.address() + ':' + port;

    // initialize node identifier
    this.id = Peer.toSha1(this.address);

    // predecessor address
    this.predecessor = null;

    // initialize finger table
    this.m = m; // finger-list-length
    this.finger = new Array(m);
    this.finger.fill('');
    this.iFinger = 0;
    fingerBase = new Array(m).fill(undefined).map((_,i) => bignum.pow(2, i));

    // initialize successor list
    this.r = r; // successor-list-length (also replica-set-size)
    this.successorList = new Array(r);
    this.successorList.fill('');
    this.successorList[0] = this.address;

    // local bucket storage
    this.bucket = {};

    this.server = new grpc.Server();
    
    this.server.addService(chordRPC.service, {
    
      echo: onEchoRequest.bind(this),
    
      ping: onPingRequest.bind(this),
    
      partition: onPartitionRequest.bind(this),
    
      merge: onMergeRequest.bind(this),
    
      get: onGetRequest.bind(this),
    
      set: onSetRequest.bind(this),

      delete: onDeleteRequest.bind(this),

      // special RPCs

      getPredecessor: onGetPredecessorRequest.bind(this),
  
      getSuccessorList: onGetSuccessorListRequest.bind(this),
  
      findSuccessor: onFindSuccessorRequest.bind(this),

      notify: onNotifyRequest.bind(this),
    
    });
    
    this.server.bind(this.address, grpc.ServerCredentials.createInsecure());

    this.server.start();

    // periodic 
    this.timeout = setInterval(() => {

      checkPredecessor.call(this);
      
      fixFingers.call(this);
      
      stabilize.call(this);

    }, 1000);

  }

  static isPort (port) {
    
    return _.isNumber(port) && (1 <= port) && (port <= 65536);

  }

  static isAddress (address) {

    if (!_.isString(address)) {
      return false;
    }

    const [ip4, port] = address.trim().split(':');

    return Peer.isPort(parseInt(port)) && ip.isV4Format(ip4);

  }

  static isPeer (object) {

    return (object instanceof Peer);
  
  }

  static isJoined (node) {

    if(!Peer.isPeer(node)) {
      throw TypeError('"node" argument must be instance of Peer');
    }
    
    return (node.address != node.successorList[0]);
  
  }

  static isBetween (el, lwr, upr, lwrIcl = false, uprIcl = false) {

    // lower before upper
    if (lwr.compare(upr) < 0) {

      // lower before el AND el before/at upper
      return (lwr.compare(el) < 0 && el.compare(upr) < 0) 
          || (lwrIcl && lwr.compare(el) === 0) 
          || (uprIcl && el.compare(upr) === 0);

    // upper before lower
    } else {

      // lower before el OR el before/at upper
      return (lwr.compare(el) < 0) || (el.compare(upr) < 0) 
          || (lwrIcl && lwr.compare(el) === 0) 
          || (uprIcl && el.compare(upr) === 0);

    }

  }

  static toSha1 (value) {
    
    return crypto.createHash('sha1').update(value).digest();

  }

}

/**
 *
 */
function rpc (receiver, method, request) {

  if (!Peer.isAddress(receiver)) {
    throw new Error('"receiver" argument must be compact IP-address:port');
  }

  // TODO
  // if (!Peer.isMethod(method)) {
  //   throw new Error('"method" argument must be valid RPC method');
  // }

  request = request || {};

  var client = new chordRPC(receiver, grpc.credentials.createInsecure());

  return new Promise((resolve, reject) => {

    client[method](request, (error, response) => {

      if (error) reject(error);

      else resolve(response);

      grpc.closeClient(client);

    });

  });

}

/**
 *
 */
async function nextLiveSuccesor () {

  var request = { sender: this.address };

  for (let address of this.successorList) {

    try {

      await rpc(address, 'ping', request);
    
      return address;

    } catch (_) {

      continue;
    
    }

  }

}

/**
 *
 */
function* closestPrecedingNode (id) {

  // iterate finger table
  for (var i = this.m - 1; i > -1; i--) {

    let finger = this.finger[i];

    // finger table is cold
    if (!Peer.isAddress(finger)) {
      continue;
    }

    let fingerId = Peer.toSha1(finger);

    // found a good finger
    if (Peer.isBetween(fingerId, this.id, id)) {
      yield finger;
    }

  }

  // no finger is applicable so default to successor list
  for (let successor of this.successorList) {
    yield successor;
  }

}

/**
 *
 */
async function findSuccessor (id) {

  // use the override switch to force this function to find next live successor for this node

  // simple optimization
  if (this.id.compare(id) === 0) {

    return { address: this.address };

  } else {

    let pingRequest = { sender: this.address };

    let findSuccessorRequest = { id };

    // TODO
    // - test the catch and continue mechanism by simulating successor failure
    for (let predecessorAddress of closestPrecedingNode.call(this, id)) {

      try {

        // found successor
        if (Peer.isBetween(id, this.id, Peer.toSha1(predecessorAddress), false, true)) {

          // check alive
          await rpc(predecessorAddress, 'ping', pingRequest);

          // break from outer loop
          return { address: predecessorAddress };

        // forward to first live successor
        } else {

          //console.log(`findSuccessor calling... ${predecessorAddress}`);

          // check alive and forward
          let findSuccessorResponse = await rpc(predecessorAddress, 'findSuccessor', findSuccessorRequest);

          // break from outer loop
          return findSuccessorResponse;

        }

      // successor is dead
      } catch (e) {

        // NOTE
        // - stabilize will clean dead successor up

        console.error('findSuccessor', e);

        // if error === Connect Failed or Timeout
        // try next successor
        continue;

        // else throw exception

      }

    }

  }

  // default
  //return { address: this.successorList[0] };

}

/**
 * 
 */
async function checkPredecessor () {

  try {

    let pingRequest = { sender: this.address };

    if (this.predecessor) await rpc(this.predecessor, 'ping', pingRequest);

  } catch (e) {

    console.error('checkPredecessor', e);

    // TODO if error === Connect Failed
    this.predecessor = null;
    // else throw error

  }
  
}

/**
 *
 */
async function fixFingers () {

  // generate finger id (this.id + 2^i % 2^160)
  var id = bignum.fromBuffer(this.id)
                 .add(fingerBase[this.iFinger])
                 .mod(MAX_ID)
                 .toBuffer();

  try {

    // find the successor for id
    var s = await findSuccessor.call(this, id);

    // update finger table entry
    this.finger[this.iFinger] = s.address;

  } catch (e) {

    console.error("fixFingers", e);

  // on success or failure
  } finally {

    // increment finger index
    this.iFinger += 1;

    // check maximum M fingers
    if (this.m <= this.iFinger) {
    
      // reset finger table index
      this.iFinger = 0;

    }

  }

}

/**
 *
 */
async function stabilize () {

  try {

    let successorAddress = this.successorList[0];

    // get successor's predecessor and call it p (potential new predecessor)
    let p = await rpc(successorAddress, 'getPredecessor');

    if (Peer.isAddress(p.address) // check p is defined and in between this node and successor
    && Peer.isBetween(Peer.toSha1(p.address), this.id, Peer.toSha1(successorAddress))) {
      
      // p is this node's successor
      successorAddress = p.address;
  
    }

    // get successor's successor list and call it l
    let l = await rpc(successorAddress, 'getSuccessorList');

    // remove last successor
    l.addresses = l.addresses.slice(0, this.r - 1);

    // prepend successor     
    // * NOTE 
    l.addresses.unshift(successorAddress);

    // NOTE
    // - record change before updating list and emitting
    // - this is false when this node just joining (positive side effect because it replicated from successor and do not want to emit)
    let changed = (this.successorList[0] != l.addresses[0]);

    // TODO
    // - handle unequal k values (received list is different size than this list)
    this.successorList = l.addresses.slice(0, this.k);

    // emit if immediate successor changed
    if (changed) {
      this.emit('successor::up', l.addresses[0]);
    }

    let notifierRequest = { sender: this.address };

    // notify successor about this (refresh its predecessor)
    await rpc(successorAddress, 'notify', notifierRequest);

  // CASE: immediate successor has died
  } catch (e) {

    //console.error('stabilize', e);

    // TODO if error === Connect Failed or Timeout {

    this.emit('successor::down', this.successorList.shift());

    // * NOTE 
    this.successorList.push(this.address);

    // NOTE
    // - the next period will stabilize the new immediate successor and emit changes

    // } else break finally and throw exception

  }

}

/**
 *
 */
function onGetPredecessorRequest (call, cb) {

  var getPredecessorResponse = {};

  if (this.predecessor != null) {
    getPredecessorResponse.address = this.predecessor;
  }

  cb(null, getPredecessorResponse);

}

/**
 *
 */
function onGetSuccessorListRequest (call, cb) {

  var getSuccessorListResponse = { addresses: this.successorList };

  cb(null, getSuccessorListResponse);

}

/**
 *
 */
async function onFindSuccessorRequest (call, cb) {

  var id = call.request.id;

  // bad request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  try {

    let findSuccessorResponse = await findSuccessor.call(this, id);

    //console.log(`onFindSuccessor return... ${findSuccessorResponse.address}`);

    cb(null, findSuccessorResponse);

  } catch (e) {

    cb(e);

  }

}

/**
 *
 */
function onPingRequest (call, cb) {

  var sender = call.request.sender;

  // bad request
  if (!Peer.isAddress(sender)) {
    return cb(new Error());
  }


  this.emit('ping', call.request);

  cb(null, call.request);

}

/**
 *
 */
function onEchoRequest (call, cb) {

  var sender = call.request.sender;

  // bad request
  if (!Peer.isAddress(sender)) {
    return cb(new Error());
  }

  this.emit('echo', call.request);

  cb(null);

}

/**
 * (see Zave 2010 rectify function)
 */
async function onNotifyRequest (call, cb) {

  // notifier's address
  let sender = call.request.sender;

  // bad request
  if (!Peer.isAddress(sender)) {
    return cb(new Error());
  }

  // if no predecessor set or is closer predecessor than the current predecessor
  if (!Peer.isAddress(this.predecessor)
  || Peer.isBetween(Peer.toSha1(sender), Peer.toSha1(this.predecessor), this.id)) {

    // update currant predecessor
    this.predecessor = sender;

  } else {

    /* current predecessor is defined */

    try {

      let pingRequest = { sender: this.address };

      await rpc(this.predecessor, 'ping', pingRequest);

      /* current predecessor is alive and valid */

    // current predecessor has died
    } catch (e) {

     // console.error('onNotifyRequest', e);
    
      // TODO if error is Connect Failed or Timeout
      this.predecessor = sender;
      // else cb(new Error());

    }
  
  }

  cb(null);

}

/**
 *
 */
function onGetRequest (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessorAddress = this.predecessor;

  // invalid predecessor state
  if (!Peer.isAddress(predecessorAddress)) {
    return cb(new Error());
  }

  var predecessorId = Peer.toSha1(predecessorAddress);

  var selfId = this.id;

  // check if key belongs to this node
  if (!Peer.isBetween(keyId, predecessorId, selfId, false, true)) {

    // TODO
    // - handle error
    // - return status code

    // invalid request
    return cb(new Error()); 

  }

  var key = keyId.toString('hex');

  if (!_.has(this.bucket, key)) {

    // invalid request
    return cb(new Error()); 
  }

  this.emit('get', { key });

  cb(null, { value: this.bucket[key] });

}

/**
 *
 */
function onSetRequest (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessorAddress = this.predecessor;

  // invalid predecessor state
  if (!Peer.isAddress(predecessorAddress)) {
    return cb(new Error());
  }

  var predecessorId = Peer.toSha1(predecessorAddress);

  var selfId = this.id;

  // check if key belongs to this node
  if (!Peer.isBetween(keyId, predecessorId, selfId, false, true)) {

    // TODO
    // - handle error
    // - return status code

    // invalid request
    return cb(new Error()); 

  }

  var key = keyId.toString('hex');

  this.bucket[key] = call.request.value;

  this.emit('set', { key });

  cb(null);

}

/**
 *
 */
function onDeleteRequest (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessorAddress = this.predecessor;

  // invalid predecessor state
  if (!Peer.isAddress(predecessorAddress)) {
    return cb(new Error());
  }

  var predecessorId = Peer.toSha1(predecessorAddress);

  var selfId = this.id;

  // check if key belongs to this node
  if (!Peer.isBetween(keyId, predecessorId, selfId, false, true)) {

    // TODO
    // - handle error
    // - return status code

    // invalid request
    return cb(new Error()); 

  }

  var key = keyId.toString('hex');

  if (!_.has(this.bucket, key)) {

    // invalid request
    return cb(new Error()); 
  }

  delete this.bucket[key];

  this.emit('delete', { key });

  cb(null);

}

/**
 * get all key-value pairs belonging to 
 */
function onPartitionRequest (call, cb) {

  var lower = call.request.lower

  var upper = call.request.upper;

  // invalid request
  if (!Buffer.isBuffer(lower)) {
    return cb(new Error());
  }

  // invalid request
  if (!Buffer.isBuffer(upper)) {
    return cb(new Error());
  }

  // NOTES
  // - when lower equals upper isBetween() always true when inclusion used
  // - prevent malicious bucket dump
  // - consider invalid request
  if (lower.compare(upper) === 0) {
    return cb(new Error());
  }

  var entries = [];

  for (let key in this.bucket) {

    let keyId = Buffer.from(key, 'hex');

    if (Peer.isBetween(keyId, lower, upper, false, true)) {

      let value = Buffer.from(this.bucket[key]);

      entries.push({ id: keyId, value });

      delete this.bucket[key];

    }

  }

  // TODO
  // - return status code
  cb(null, { entries });

}

/**
 *
 */
function onMergeRequest (call, cb) {

  var entries = call.request.entries;

  // invalid request
  if (!_.isArray(entries)) {
    return cb(new Error());
  }

  var predecessorAddress = this.predecessor;

  // invalid predecessor state
  if (!Peer.isAddress(predecessorAddress)) {
    return cb(new Error());
  }

  var predecessorId = Peer.toSha1(predecessorAddress);  

  var selfId = this.id;

  for (let entry of entries) {

    let keyId = entry.id;

    if (Peer.isBetween(keyId, predecessorId, selfId, false, true)) {

      let key = keyId.toString('hex');

      this.bucket[key] = entry.value;

    }

  }
  
  // TODO
  // - return status code
  cb(null);

}

module.exports = {
  Peer,
  rpc
}
