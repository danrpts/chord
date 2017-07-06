'use strict';

/**
 * IMPORT DEPENDENCIES
 */
const _ = require('underscore');
const ip = require('ip');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const grpc = require('grpc');
const endpoints = require('./endpoints.js');
const util = require('./util.js');
const isPort = util.isPort;
const isAddress = util.isAddress;
const isBetween = util.isBetween;
const toSha1 = util.toSha1;
const doRpc = util.doRpc;

/**
 * DEFINE CONSTANTS
 */
const CHORD_SUCCESSOR_LENGTH = 1; // default successor-list-length and replica-set-size
const CHORD_FINGER_LENGTH = 160; // default finger-list length (should be 160 for sha1)
const CHORD_FINGER_BASE = Array(CHORD_FINGER_LENGTH).fill(0).map((_, i) => bignum.pow(2, i)); // powers of 2 closure
const CHORD_MAX_ID = bignum.pow(2, 160); // 160 because using SHA1 hash function
const CHORD_PROTO_PATH = '/chord.proto';
const CHORD_PROTO_GRPC = grpc.load(__dirname + CHORD_PROTO_PATH).CHORD_PROTO;


/**
 *
 */
const maintenance = (() => {

  // timeout closure
  var timeout;

  // return maintenance API
  return {

    start: function () {
     timeout = setInterval(async () => {
        await checkPredecessor.call(this);
        await fixFingers.call(this);
        await stabilize.call(this);
      }, 1000);
    },

    stop: function () {
      clearInterval(timeout);
      timeout = undefined;
    }

  }

})();

/**
 * Fix successor's predecessor
 */
const checkPredecessor = async function () {

  try {

    if (isAddress(this.predecessor)) {
      await doRpc(this.predecessor, 'ping', { 
        sender: this.address
      });
    }

  } catch (e) {

    console.error('checkPredecessor', e);

    // TODO if error === Connect Failed
    this.predecessor = undefined;
    // else throw error

  }
  
}

/**
 * Fix this finger table
 */
const fixFingers = (() => {

  // index closure
  var i = 0;

  // do not use arrow function so context can be bound
  return async function () {

    try {

      // generate finger id (this.id + 2^i % 2^160)
      let id = bignum.fromBuffer(this.id)
                     .add(CHORD_FINGER_BASE[i])
                     .mod(CHORD_MAX_ID)
                     .toBuffer();

      // find the successor for id
      let lookupResponse = await doRpc(this.address, 'lookup', { id });

      let successor0 = lookupResponse.successor;

      // invalid response
      if (!isAddress(successor0)) {
        throw new Error();
      }

      // update finger table entry
      this.finger[i] = successor0;

    } catch (e) {

      console.error("fixFingers", e);

    // on success or failure
    } finally {

      // increment finger index
      i += 1;

      // check maximum M fingers
      if (CHORD_FINGER_LENGTH <= i) {
      
        // reset finger table index
        i = 0;

      }

    }

  }

})();

/**
 * Fix this successor list
 */
const stabilize = async function () {

  var done = false;

  var i = 0;

  // NOTEs
  // - see Zave 2010 StabilizeFromSuccessor step
  // - every doRPC is also a check for liveliness
  do {
  
    try {

      // define immediate successor
      let successor0 = this.successor[i];

      // invalid state
      if (!isAddress(successor0)) {
        throw new Error('invalid state -- no immediate successor');
      }

      // fetch immediate successor's predecessor and successor list (potentially the new successor)
      let stateResponse = await doRpc(successor0, 'state', {
        predecessor: true,
        successor: true
      });

      // update the successor list (fix successor list)
      let updatedSuccessor = stateResponse.successor;
      updatedSuccessor = updatedSuccessor.slice(0, this.r - 1);
      updatedSuccessor.unshift(successor0);

      // predecessor address (idealize)
      let potentialSuccessor0 = stateResponse.predecessor;

      // NOTEs
      // - see Zave 2010 StabilizeFromPredecessor step
      // - true when potentialSuccessor0 just joined
      if (isAddress(potentialSuccessor0)
        && isBetween(this.id, toSha1(potentialSuccessor0), toSha1(successor0))) {

          // re-fetch because immediate successor changed and knows most up to date successor list
          stateResponse = await doRpc(potentialSuccessor0, 'state', {
            successor: true
          });

          // re-update the successor list
          updatedSuccessor = stateResponse.successor;
          updatedSuccessor = updatedSuccessor.slice(0, this.r - 1);
          updatedSuccessor.unshift(potentialSuccessor0);

      }

      // record successors being removed
      let down = _.chain(this.successor).difference(updatedSuccessor).without(this.address).value();

      // record successors being added
      let up = _.chain(updatedSuccessor).difference(this.successor).without(this.address).value();

      this.successor = updatedSuccessor;

      done = true;

      if (!_.isEmpty(down)) {
        this.emit('successor::down', down);
      }

      if (!_.isEmpty(up)) {
        this.emit('successor::up', up);
      }

    // immediate successor has died
    } catch (e) {

      console.error('stabilize', e);

      i++;

    }

  } while (!done && i < this.successor.length);

  // NOTEs
  // - special case when successor list is exhausted and has stabilization failed (due to involuntary failures)
  // - occurs when r = 1 and must repair an involuntary node failure
  // ... or (with low probability) r > 1 and all successors fail involuntary
  // - the next stabilize call will then repair it using StabilizeFromPredecessor step
  // - this has an effect where the immediate successor points back to the predecessor temporarily
  // ... so be aware of this for partition replication (successor up/down events)
  // TODOs
  // - may want to use a timeout and allow the failed node to come back online
  if (!done) {
    
    /* assert(i === this.successor.length) */

    this.emit('successor::down', this.successor);
    this.successor.fill(this.address);

  }

  // notify immediate successor (use list value - may be updated) about this node (refresh its predecessor)
  await doRpc(this.successor[0], 'notify', {
    sender: this.address
  });

}

/**
 * DEFINE PEER CLASS
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port, r) {

    super();

    if (!isPort(port)) {
      throw new Error('"port" argument must be number between 1 and 65536');
    }

    // set IP address
    this.address = ip.address() + ':' + port;

    // initialize identifier
    this.id = toSha1(this.address);

    // predecessor address
    this.predecessor = null;

    // initialize successor list
    this.r = (r > 0) ? r : CHORD_SUCCESSOR_LENGTH;
    this.successor = new Array(r);
    this.successor.fill(this.address);

    // initialize finger table
    this.finger = new Array(CHORD_FINGER_LENGTH);
    this.finger.fill(this.address);
    
    // bind endpoints to peer context and give to gRPC server
    this.server = new grpc.Server();
    this.server.addService(CHORD_PROTO_GRPC.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));
    
    this.server.bind(this.address, grpc.ServerCredentials.createInsecure());

    this.server.start();

    maintenance.start.call(this);

  }

  /**
   *
   */
  static isPeer (object) {

    return (object instanceof Peer);

  }

  /**
   *
   */
  static isJoined (object) {

    if(!Peer.isPeer(object)) {
      throw TypeError('"object" argument must be instance of Peer');
    }

    if (!isAddress(object.address)) {
      throw new Error();
    }

    if (!isAddress(object.successor[0])) {
      throw new Error();
    }
    
    return (object.address != object.successor[0]);

  }

  /**
   * Search the finger table and successor list for an address that minimally precedes id
   */
  *closestPrecedingNode (keyId) {

    // iterate finger table
    for (var i = CHORD_FINGER_LENGTH - 1; i > -1; i--) {

      let fingeri = this.finger[i];

      // finger table is cold
      if (!isAddress(fingeri)) {
        continue;
      }

      // found a good finger
      if (isBetween(this.id, toSha1(fingeri), keyId)) {
        yield fingeri;
      }

    }

    // no finger is applicable so default to successor list
    // NOTEs
    // - in the case where we are called from stabilize, and list length is 1, and the list is empty
    // - and all fingers are bad, we skip this loop and return address of this
    for (let successor of this.successor) {

      if (!isAddress(successor)) {
        continue;
      }     

      yield successor;

    }

    // impossible case as successor[0] *must* always be defined unless error
    return this.address;

  }

  /**
   * Ping a host from this peer
   */
  async ping (host) {

    // invalid request
    if (!isAddress(host)) {
      throw new Error('"host" argument must be compact IP-address:port');
    }

    return doRpc(host, 'ping', {
      sender: this.address
    });

  }

  /**
   *  Request the state associated of a host
   */
  async state (host) {

    // invalid request
    if (!isAddress(host)) {
      throw new Error('"host" argument must be compact IP-address:port');
    }

    return doRpc(host, 'state', {
      sender: this.address
    });

  }

  /**
   * 
   */
  async lookup (key) {

    // invalid request
    if (!_.isString(key)) {
      throw new Error('"key" argument must be string');
    }

    var id = toSha1(key);

    return doRpc(this.address, 'lookupResponse', { id });

  }

  /**
   * Join a Chord overlay network via a host node
   */
  async join (host) {

    // invalid state
    if (Peer.isJoined(this)) {
      throw new Error('peer must not be joined');
    }

    // invalid request
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    // invalid request
    if (host === this.address) {
      throw new Error('cannot join to this');
    }

    try {

      maintenance.stop();

      let id = toSha1(this.address);
   
      let lookupResponse = await doRpc(host, 'lookup', { id });

      let newSuccessor0 = lookupResponse.successor;

      // invalid response
      if (!isAddress(newSuccessor0)) {
        throw new Error();
      }

      let stateResponse = await doRpc(newSuccessor0, 'state', {
        predecessor: true,
        successor: true,
        finger: true
      });

      // predecessor address
      let newPredecessor = stateResponse.predecessor;

      // invalid response
      if (!isAddress(newPredecessor)) {
        throw new Error();
      }

      // get successor list
      let updatedSuccessor = stateResponse.successor;
      updatedSuccessor = updatedSuccessor.slice(0, this.r - 1);
      updatedSuccessor.unshift(newSuccessor0);
      this.successor = updatedSuccessor;

    } catch (e) {

      throw e;

    } finally {

      maintenance.start.call(this);

    }

  }

  /**
   * Leave the Chord overlay network this peer is joined to
   */
  async leave () {

    if (!Peer.isJoined(this)) {
      throw new Error('peer must be joined');
    }

    try {

      maintenance.stop();

      // reset predecessor
      this.predecessor = null;

      // reset successor list
      this.successor.fill(this.address);

      this.finger.fill(this.address);

      // TODOs
      // - first on this: send s = this.successorList[this.k - 1] to predecessor
      // - then on predecessor: remove s from successor list
      // - then on predecessor: push (last successor) to successor list
      
      // TODOs
      // - first on this: send p = this.predecessor to successor
      // - then on successor: replace predecessor with p
      
      this.server.tryShutdown(()=>{});

    } catch (e) {

      throw e;

    }

  }  

}

/**
 * EXPOSE PEER CLASS
 */
module.exports = {
  Peer
};
