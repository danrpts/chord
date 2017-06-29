'use strict';
const _ = require('underscore');
const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const Buffer = require('buffer').Buffer;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

// finger list length (should be hash length in bits e.g. 160 fro sha1)
const M = 4; 

// 160 because using SHA1 hash function
const MAX_ID = bignum.pow(2, 160);

// precomputed powers of 2
const FINGER_BASE = new Array(M).fill(undefined).map((_,i) => bignum.pow(2, i));

// default successor list length and replica set size
const R = 1;

// TODO
// - return own status codes
// - handle gRPC errors
const STATUS_CODES = {};

/**
 *
 */
function isPort (port) {
  
  return _.isNumber(port) && (1 <= port) && (port <= 65536);

}

/**
 *
 */
function isAddress (address) {

  if (!_.isString(address)) return false;

  const [ip4, port] = address.trim().split(':');

  return isPort(parseInt(port)) && ip.isV4Format(ip4);

}

/**
 *
 */
function get (peer, key) {

  if (!Peer.isPeer(peer) && !Peer.isJoined(peer)) {
    return new Error('"peer" argument must be joined peer');
  }

  if (!isString(key)) {
      return new Error('"key" argument must be string');
  }

  // TODO
  // - use as DHT api instead of peer methods

}

/**
 *
 */
function set (peer, key, value) {

  if (!isPeer(peer) && !Peer.isJoined(peer)) {
    return new Error('"peer" argument must be joined peer');
  }


  if (!isString(key)) {
      return new Error('"key" argument must be string');
  }

  if (!Buffer.isBuffer(value)) {
      return new Error('"value" argument must be buffer');
  } 

  // TODO
  // - use as DHT api instead of peer methods

}

/**
 *
 */
function rpc (address, method, request) {

  if (!isAddress(address)) {

    // TODO
    // - throw or return (research error handling)
    return new Error('"address" argument must be compact IP-address:port');
  
  }

  request = request || {};

  var client = new chordRPC(address, grpc.credentials.createInsecure());

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
function toSha1 (value) {
  
  return crypto.createHash('sha1').update(value).digest();

}

/**
 *
 */
function between (el, lwr, upr, lwrIcl = false, uprIcl = false) {

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

/**
 * 
 */
async function checkPredecessor () {

  try {

    if (this.predecessor) await rpc(this.predecessor, 'ping');

  } catch (err) {

    console.error('checkPredecessor', err);

    // TODO if err === Connect Failed
    this.predecessor = null;
    // else throw error

  }
  
}

/**
 *
 */
async function fixFingers () {

  // generate finger id (this.id + 2^k % 2^160)
  var id = bignum.fromBuffer(this.id)
                 .add(FINGER_BASE[this.iFinger])
                 .mod(MAX_ID)
                 .toBuffer();
  
  try {

    // find the successor for id
    var s = await findSuccessor.call(this, id);

    // update finger table entry
    this.finger[this.iFinger] = s.address;

  } catch (err) {

    console.error("fixFingers", err);

  // on success or failure
  } finally {

    // increment finger index
    this.iFinger += 1;

    // check maximum M fingers
    if (M <= this.iFinger) {
    
      // reset finger table index
      this.iFinger = 0;

    }

  }

}

/**
 *
 */
async function rectify (notifier) {

  var predecessorIsDefined = isAddress(this.predecessor);

  try {
    
    // check predecessor is alive
    if (predecessorIsDefined) {

      await rpc(this.predecessor, 'ping');
    
    }
  
  } catch (err) {

    console.error('rectify', err);
  
    // TODO if err is Connect Failed or Timeout
    
    this.predecessor = null;

    predecessorIsDefined = false;
    
    // else break finally and throw exception

  } finally {

    // if no predecessor set
    if (!predecessorIsDefined

    // or if notifier is a closer predecessor than current 
    || between(toSha1(notifier.address), toSha1(this.predecessor), this.id)) {

      // update currant predecessor
      this.predecessor = notifier.address;
    
    }

  }

}

/**
 *
 */
async function stabilize () {

  try {

    let successor = this.successorList[0];

    // get successor's predecessor and call it p (potential new predecessor)
    let p = await rpc(successor, 'getPredecessor');

    if (isAddress(p.address) // check p is defined and in between this node and successor
    && between(toSha1(p.address), this.id, toSha1(successor))) {
      
      // p is this node's successor
      successor = p.address;
  
    }

    // get successor's successor list and call it l
    let l = await rpc(successor, 'getSuccessorList');

    // remove last successor
    l.addrs = l.addrs.slice(0, this.k - 1);

    // prepend successor     
    // * NOTE 
    l.addrs.unshift(successor);

    // NOTE
    // - record change before updating list and emitting
    // - this is false when this node just joining (positive side effect because it replicated from successor and do not want to emit)
    let changed = (this.successorList[0] != l.addrs[0]);

    // borrow p's list l (at most k)
    for (let i = 0; i < this.k; i++) {
      
      // l has fewer elements (at least 1)
      if (l.addrs.length <= i)  {
    
        // * NOTE 
        this.successorList[i] = this.address;
      
      } else {

        this.successorList[i] = l.addrs[i];

      }

      this.successorList[i] = l.addrs[i];

    }

    // emit if immediate successor changed
    if (changed) {
      this.emit('successor', l.addrs[0]);
    }

    // notify successor about this (refresh its predecessor)
    await rpc(successor, 'notify', { address: this.address });

  // CASE: immediate successor has died
  } catch (err) {

    console.error('stabilize', err);

    // TODO if err === Connect Failed or Timeout {

    this.successorList.shift();

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
function* closestPrecedingNode (id) {

  // iterate finger table
  for (var i = M - 1; i > -1; i--) {

    let finger = this.finger[i];

    // finger table is cold
    if (!isAddress(finger)) continue;

    let fingerId = toSha1(finger);

    // found a good finger
    if (between(fingerId, this.id, id)) {

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

  // simple optimization
  if (this.id.compare(id) === 0) {

    return { address: this.address };

  } else {

    for (let predecessorAddr of closestPrecedingNode.call(this, id)) {

      // found successor
      if (between(id, this.id, toSha1(predecessorAddr), false, true)) {

        try {

          // check alive
          await rpc(predecessorAddr, 'ping');

          // break from outer loop
          return { address: predecessorAddr };

        // successor is dead
        } catch (err) {

          // if err === Connect Failed or Timeout
          // try next successor
          continue;
          // else throw exception

        }

      // forward to first live successor
      } else {

        try {

          //console.log(`findSuccessor calling... ${predecessorAddr}`);

          // check alive and forward
          let findSuccessorResponse = await rpc(predecessorAddr, 'findSuccessor', { id });

          // break from outer loop
          return findSuccessorResponse;

        // successor is dead
        } catch (err) {

          // if err === Connect Failed or Timeout
          // try next successor
          continue;
          // else throw exception

        }

      }

    }

  }

  // default
  //return { address: this.successorList[0] };

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

  cb(null, { addrs: this.successorList });

}

/**
 *
 */
async function onFindSuccessorRequest (call, cb) {

  try {

    let findSuccessorResponse = await findSuccessor.call(this, call.request.id);

    //console.log(`onFindSuccessor return... ${findSuccessorResponse.address}`);

    cb(null, findSuccessorResponse);

  } catch (err) {

    cb(err);

  }

}

/**
 *
 */
function onEchoRequest (call, cb) {

  this.emit('echo', call.request);

  cb(null);

}

/**
 *
 */
function onPingRequest (call, cb) {

  cb(null, call.request);

}

/**
 *
 */
function onNotifyRequest (call, cb) {

  try {
    
    rectify.call(this, call.request);

    // TODO
    // - return status code
  
    cb(null);

  } catch (err) {

    cb(err);

  }

}

/**
 *
 */
function onGetRequest (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');

  if (_.has(this.bucket, idStr)) {

     // return value as buffer
    cb(null, { value: this.bucket[idStr] });

  } else {

    // TODO
    // - handle error
    // - return status code

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onSetRequest (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');
  
  // type of value is Buffer
  var value = call.request.value;

  this.bucket[idStr] = value;

  this.emit('set', {id, value});

  // TODO
  // - handle error
  // - return status code

  cb(null); 

}

/**
 *
 */
function onDeleteRequest (call, cb) {

  var id = call.request.id;

  var idStr = id.toString('hex');

  if (this.bucket.hasOwnProperty(idStr)) {

    delete this.bucket[idStr];

    // TODO
    // - return status code

    cb(null);

  } else {

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onGetAllRequest (call, cb) {

  var bucketEntries = [];

  var pId = toSha1(this.predecessor);

  var nId = call.request.id;

  for (let idStr in this.bucket) {

    let kId = Buffer.from(idStr, 'hex');

    if (between(kId, pId, nId, false, true)) {

      let value = Buffer.from(this.bucket[idStr], 'utf8');

      bucketEntries.push({ id, value });

      delete this.bucket[idStr];

    }

  }

  // TODO
  // - return status code
  cb(null, bucketEntries);

}

/**
 *
 */
function onSetAllRequest (call, cb) {

  var bucketEntries = call.request.bucketEntries;

  var pId = toSha1(this.predecessor);

  var nId = this.id;

  for (let entry of bucketEntries) {

    let kId = entry.id;

    if (between(kId, pId, nId, false, true)) {
    
      let value = entry.value;

      let kIdStr = kId.toString('hex');

      this.bucket[kIdStr] = value.toString('utf8');

    }

  }
  
  // TODO
  // - return status code
  cb(null);

}

/**
 *
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port, k) {

    super();

    // TODO
    // - use random port when port is 0

    if (!isPort(port)) {
      throw new Error('"port" argument must be between 1 and 65536');
    }

    // set address
    this.address = ip.address() + ':' + port;

    // initialize node identifier
    this.id = toSha1(this.address);
    
    // successor list length and replica set size
    this.k = k;

    // predecessor address
    this.predecessor = null;

    // successor list
    this.successorList = new Array(k);

    this.successorList.fill('');

    // immediate successor
    this.successorList[0] = this.address;

    // local bucket storage
    this.bucket = {};

    this.finger = new Array(M);

    this.finger.fill('');

    this.iFinger = 0;

    this.server = new grpc.Server();
    
    this.server.addService(chordRPC.service, {
  
      getPredecessor: onGetPredecessorRequest.bind(this),
  
      getSuccessorList: onGetSuccessorListRequest.bind(this),
  
      findSuccessor: onFindSuccessorRequest.bind(this),
    
      echo: onEchoRequest.bind(this),
    
      ping: onPingRequest.bind(this),
    
      notify: onNotifyRequest.bind(this),
    
      get: onGetRequest.bind(this),
    
      set: onSetRequest.bind(this),

      delete: onDeleteRequest.bind(this),     
    
      getAll : onGetAllRequest.bind(this),
    
      setAll: onSetAllRequest.bind(this)
    
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

  /**
   *
   */
  async echo (address, message) {

    var getEchoResponse;

    try {

      getEchoResponse = await rpc(address, 'echo', { message, address: this.address });

    } catch (err) {

      console.error('echo', err);

    }

    return getEchoResponse;

  }

  /**
   *
   */
  async ping (address) {

    var getPingResponse;

    try {

      getPingResponse = await rpc(address, 'ping');

    } catch (err) {
    
      console.error('ping', err);

    }

    return getPingResponse;

  }

  /**
   *
   */
  async join (address) {

    // check if already joined
    if (this.successorList[0] != this.address) {
      return;
    }

    // bootstrap steps setup successor list (do not wait for stabilize)
    try {

      // get this node's successor and call it s
      let s = await rpc(address, 'findSuccessor', { id: this.id });

      this.replicateFrom(s.address);

      // get s's predecessor and call is p
      let p = await rpc(s.address, 'getPredecessor');

      /* invariant: p is this node's predecessor */

      // get p's successor list and call it l
      let l = await rpc(p.address, 'getSuccessorList');

      // set p as predecessor
      this.predecessor = p.address;

      // borrow p's list l (at most k)
      for (let i = 0; i < this.k; i++) {
        
        // l has fewer elements (at least 1)
        if (l.addrs.length <= i)  {
        
          // * NOTE 
          this.successorList[i] = this.address;
        
        } else {

          this.successorList[i] = l.addrs[i];

        }

      }

      // NOTE
      // - do not emit successor change because we replicated from it

    } catch (err) {
    
      console.error('join', err);

    }

  }

  /**
   *
   */
  async leave () {

    // check if not joined
    if (this.successorList[0] === this.address) {
      return;
    }

    // TODO
    // - get live successor
    await this.replicateTo(this.successorList[0]);

    // reset predecessor
    this.predecessor = null;

    // reset successor list
    this.successorList.fill('');

    // reset immediate successor
    this.successorList[0] = this.address;

    // TODO
    // - first on self: send s = this.successorList[this.k - 1] to predecessor
    // - then on predecessor: remove s from successor list
    // - then on predecessor: push (last successor) to successor list
    
    // TODO
    // - first on self: send p = this.predecessor to successor
    // - then on successor: replace predecessor with p

  }

  /**
   *
   */
  async get (key) {

    var id = toSha1(key);

    var getResponse;
    
    try {

      let s = await findSuccessor.call(this, id);

      getResponse = await rpc(s.address, 'get', { id });

    } catch (err) {
    
      console.log('get', err);

    }

    return getResponse;

  }

  /**
   *
   */
  async set (key, value) {

    var id = toSha1(key);

    // TODO
    // - check value is buffer

    try {

      let s = await findSuccessor.call(this, id);

      await rpc(s.address, 'set', { id, value });

    } catch (err) {
    
      console.error('set', err);

    }

  }

  /**
   *
   */
  async delete (key) {

    var id = toSha1(key);

    var deleteResponse;

    try {

      let s = await findSuccessor.call(this, id);

      deleteResponse = await rpc(s.address, 'delete', { id });

    } catch (err) {
    
      console.error("delete", err);

    }

    return deleteResponse;

  }

  /**
   *
   */
  async replicateFrom (address) {

    if (address === this.address) {
      return;
    }

    try {

      let getAllResponse = await rpc(address, 'getAll', { id: this.id });

      for (let entry of getAllResponse.bucketEntries) {

        let idStr = entry.id.toString('hex');

        this.bucket[idStr] = entry.value;

      }

    } catch (err) {

      console.error('replicateFrom', err);

    }

  }

  /**
   *
   */
  async replicateTo (address) {

    if (address === this.address) {
      return;
    }

    try {

      let bucketEntries = [];

      for (let idStr in this.bucket) {
        
        let id = Buffer.from(idStr, 'hex');
        
        let value = this.bucket[idStr];
        
        bucketEntries.push({ id, value });

      }

      await rpc(address, 'setAll', { bucketEntries });

    } catch (err) {

      console.error('replicateTo', err);

    }

  }

  /**
   *
   */
  async close () {

    await this.leave();

    // TODO
    // - unbind events

    clearInterval(this.timeout);

    this.server.tryShutdown(err => {

      if (err) console.error(err);

    });
  
  }

  static isPeer (peer) {
    return (peer instanceof Peer);
  }

  static isJoined (peer) {
    return this.address != this.successorList[0];
  }

}

/**
 *
 */
function createPeer (port, nSuccessors) {

  nSuccessors = (nSuccessors > 0) ? nSuccessors : R;

  return new Peer(port, nSuccessors);

}

module.exports = {
  STATUS_CODES,
  createPeer,
  Peer,
  isPort,
  isAddress,
  get,
  set
}
