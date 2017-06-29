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

    // finger-list-length
    this.m = m;

    fingerBase = new Array(m).fill(undefined).map((_,i) => bignum.pow(2, i));

    // successor-list-length (also replica-set-size)
    this.r = r;

    // predecessor address
    this.predecessor = null;

    // initialize finger table
    this.finger = new Array(m);
    this.finger.fill('');
    this.iFinger = 0;

    // successor list
    this.successorList = new Array(r);

    this.successorList.fill('');

    // immediate successor
    this.successorList[0] = this.address;

    // local bucket storage
    this.bucket = {};

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
    this.timeout = setInterval( () => {

      checkPredecessor.call(this);
      
      fixFingers.call(this);
      
      stabilize.call(this);

    }, 1000);

  }

  static isPort (port) {
    
    return _.isNumber(port) && (1 <= port) && (port <= 65536);

  }

  static isAddress (address) {

    if (!_.isString(address)) return false;

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
function rpc (address, method, request) {

  if (!Peer.isAddress(address)) {
    throw new Error('"address" argument must be compact IP-address:port');
  }

  // if (!Peer.isMethod(method)) {
  //   throw new Error('"method" argument must be valid RPC method');
  // }

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

async function nextLiveSuccesor () {

  var request = { address: this.address };

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
async function findSuccessor (id) {

  // use the override switch to force this function to find next live successor for this node

  // simple optimization
  if (this.id.compare(id) === 0) {

    return { address: this.address };

  } else {

    // TODO
    // - test the catch and continue mechanism by simulating successor failure
    for (let predecessor of closestPrecedingNode.call(this, id)) {

      try {

        // found successor
        if (Peer.isBetween(id, this.id, Peer.toSha1(predecessor), false, true)) {

          // check alive
          await rpc(predecessor, 'ping', { address: this.address });

          // break from outer loop
          return { address: predecessor };

        // forward to first live successor
        } else {

          //console.log(`findSuccessor calling... ${predecessor}`);

          // check alive and forward
          let findSuccessorResponse = await rpc(predecessor, 'findSuccessor', { id });

          // break from outer loop
          return findSuccessorResponse;

        }

      // successor is dead
      } catch (error) {

        console.error('findSuccessor', error);

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

    if (this.predecessor) await rpc(this.predecessor, 'ping', { address: this.address });

  } catch (error) {

    console.error('checkPredecessor', error);

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

  } catch (error) {

    console.error("fixFingers", error);

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

    let successor = this.successorList[0];

    // get successor's predecessor and call it p (potential new predecessor)
    let p = await rpc(successor, 'getPredecessor');

    if (Peer.isAddress(p.address) // check p is defined and in between this node and successor
    && Peer.isBetween(Peer.toSha1(p.address), this.id, Peer.toSha1(successor))) {
      
      // p is this node's successor
      successor = p.address;
  
    }

    // get successor's successor list and call it l
    let l = await rpc(successor, 'getSuccessorList');

    // remove last successor
    l.addrs = l.addrs.slice(0, this.r - 1);

    // prepend successor     
    // * NOTE 
    l.addrs.unshift(successor);

    // NOTE
    // - record change before updating list and emitting
    // - this is false when this node just joining (positive side effect because it replicated from successor and do not want to emit)
    let changed = (this.successorList[0] != l.addrs[0]);

    // borrow p's list l (at most r)
    for (let i = 0; i < this.r; i++) {
      
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
  } catch (e) {

    //console.error('stabilize', e);

    // TODO if error === Connect Failed or Timeout {

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

  } catch (e) {

    cb(e);

  }

}


/**
 *
 */
function onPingRequest (call, cb) {

  if (Peer.isAddress(call.request.address)) {

    this.emit('ping', call.request);

    cb(null, call.request);

  } else {

    // malformed request
    cb(new Error());

  }

}

/**
 *
 */
function onEchoRequest (call, cb) {

  if (Peer.isAddress(call.request.address)) {

    this.emit('echo', call.request);

    cb(null);

  } else {

    // malformed request
    cb(new Error());

  }

}

/**
 *
 */
async function onNotifyRequest (call, cb) {

  // (see Zave paper rectify function)

  var predecessorIsDefined = Peer.isAddress(this.predecessor);

  try {
    
    // check predecessor is alive
    if (predecessorIsDefined) {

      await rpc(this.predecessor, 'ping', { address: this.address });
    
    }
  
  } catch (error) {

    console.error('onNotifyRequest', error);
  
    // TODO if error is Connect Failed or Timeout
    
    this.predecessor = null;

    predecessorIsDefined = false;
    
    // else break finally and throw exception

  } finally {

    // notifier's address
    let address = call.request.address;

    // if no predecessor set
    if (!predecessorIsDefined

    // or if notifier is a closer predecessor than the current predecessor
    || Peer.isBetween(Peer.toSha1(address), Peer.toSha1(this.predecessor), this.id)) {

      // update currant predecessor
      this.predecessor = address;
    
    }

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

    this.emit('get', { id });

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

  this.emit('set', { id });

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

  if (_.has(bucket, idStr)) {

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

  var pId = Peer.toSha1(this.predecessor);

  var nId = call.request.id;

  for (let idStr in this.bucket) {

    let kId = Buffer.from(idStr, 'hex');

    if (Peer.isBetween(kId, pId, nId, false, true)) {

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

  var pId = Peer.toSha1(this.predecessor);

  var nId = this.id;

  for (let entry of bucketEntries) {

    let kId = entry.id;

    if (Peer.isBetween(kId, pId, nId, false, true)) {
    
      let value = entry.value;

      let kIdStr = kId.toString('hex');

      this.bucket[kIdStr] = value.toString('utf8');

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
