'use strict';

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

const MIN_SUCCESSOR_LENGTH = 1; // default successor-list-length and replica-set-size
const FINGER_LENGTH = 160; // default finger-list length (should be 160 for sha1)
const FINGER_BASE = Array(FINGER_LENGTH).fill(0).map((_, i) => bignum.pow(2, i)); // powers of 2 closure
const MAX_ID = bignum.pow(2, 160); // 160 because using SHA1 hash function
const CHORD_PROTO_PATH = '/chord.proto';
const CHORD_PROTO_GRPC = grpc.load(__dirname + CHORD_PROTO_PATH).CHORD_PROTO;


/**
 * Schedule the maintenance procedures.
 * @return {object}
 */
const maintenance = (() => {

  // timeout closure
  var timeout;

  // return API
  return {

    start: function () {
     timeout = setInterval(async () => {
        await fixPredecessor.call(this);
        await fixFinger.call(this);
        await fixSuccessor.call(this);
      }, 1000);
    },

    stop: function () {
      clearInterval(timeout);
      timeout = undefined;
    }

  }

})();

/**
 * Maintain the predecessor. (A.K.A. checkPredecessor)
 */
const fixPredecessor = async function () {

  try {

    if (isAddress(this.predecessor)) {
      await doRpc(this.predecessor, 'ping', { 
        sender: this.address
      });
    }

  } catch (e) {

    console.error('fixPredecessor', e);

    // TODO if error === Connect Failed
    this.predecessor = undefined;
    // else throw error

  }
  
}

/**
 * Maintain the finger table entries.
 */
const fixFinger = (() => {

  // finger index closure
  var i = 0;

  // do not use arrow function so context can be bound
  return async function () {

    try {

      // generate finger id
      let id = bignum.fromBuffer(this.id)
                     .add(FINGER_BASE[i])
                     .mod(MAX_ID)
                     .toBuffer();

      // find successor
      let lookupResponse = await doRpc(this.address, 'lookup', { id });

      // immediate successor address
      let successor0 = lookupResponse.successor;

      // invalid successor response
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
      if (FINGER_LENGTH <= i) {
      
        // reset finger table index
        i = 0;

      }

    }

  }

})();

/**
 * Maintain the successor list entries. (A.K.A. stabilize)
 */
const fixSuccessor = async function () {

  var done = false;

  // successor index
  var i = 0;

  // NOTEs
  // - see Zave 2010 StabilizeFromSuccessor step
  do {
  
    try {

      // grab next immediate successor
      let successor0 = this.successor[i];

      // get predecessor, which is potentially the new successor, and successor list
      let stateResponse = await doRpc(successor0, 'state', {
        predecessor: true,
        successor: true
      });

      // pre-adopt the successor list
      let newSuccessor = stateResponse.successor;
      newSuccessor = newSuccessor.slice(0, this.r - 1);
      newSuccessor.unshift(successor0);

      // potential successor address (A.K.A. idealize)
      let potentialSuccessor0 = stateResponse.predecessor;

      // NOTEs
      // - see Zave 2010 StabilizeFromPredecessor step
      // - true if potentialSuccessor0 just joined
      if (isAddress(potentialSuccessor0)
        && isBetween(this.id, toSha1(potentialSuccessor0), toSha1(successor0))) {

        try {

          // re-fetch because immediate successor has changed
          stateResponse = await doRpc(potentialSuccessor0, 'state', {
            successor: true
          });

          // pre-adopt the successor list
          newSuccessor = stateResponse.successor;
          newSuccessor = newSuccessor.slice(0, this.r - 1);
          newSuccessor.unshift(potentialSuccessor0);

        } catch (_) {

          // potentialSuccessor0 has died while joining so continue successor0

        }

      }

      // record successors being removed
      let down = _.chain(this.successor).difference(newSuccessor).without(this.address).value();

      // record successors being added
      let up = _.chain(newSuccessor).difference(this.successor).without(this.address).value();

      // fully adopt the sucessor list
      this.successor = newSuccessor;

      // successor list has been refreshed
      done = true;

      // bubbled downed successors to higher-layer
      if (!_.isEmpty(down)) {
        this.emit('successor::down', down);
      }

      // bubbled upped successors to higher-layer
      if (!_.isEmpty(up)) {
        this.emit('successor::up', up);
      }

    } catch (_) {

      // immediate successor had died move to next in list
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

    // bubbled downed successors to higher-layer
    this.emit('successor::down', this.successor);

    // reset successor list
    this.successor.fill(this.address);

  }

  try {

    // notify immediate successor (refresh its predecessor)
    await doRpc(this.successor[0], 'notify', {
      sender: this.address
    });

  } catch (_) {

    // ignore failure

  }

}

/** 
 * Class representing a Chord peer. 
 * @extends EventEmitter
 */
class Peer extends EventEmitter {

  /**
   * Create an instance of Peer.
   * @param {number} port - The port to bind the peer to.
   * @param {object} options - The optional arguments.
   */
  constructor (port, options) {

    super();

    _.defaults(options, {

      nSuccessors: MIN_SUCCESSOR_LENGTH

    });

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
    this.r = options.nSuccessors;
    this.successor = new Array(this.r);
    this.successor.fill(this.address);

    // initialize finger table
    this.finger = new Array(FINGER_LENGTH);
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
   * Test if an object is an instance of Peer.
   * @param {object} object -
   * @return {boolean}
   */
  static isPeer (object) {

    return (object instanceof Peer);

  }

  /**
   * Test if a peer is joined to a Chord overlay network.
   * @param {Peer} peer -
   * @return {boolean}
   */
  static isJoined (peer) {

    if(!Peer.isPeer(peer)) {
      throw TypeError('"peer" argument must be instance of Peer');
    }

    if (!isAddress(peer.address)) {
      throw new Error();
    }

    if (!isAddress(peer.successor[0])) {
      throw new Error();
    }
    
    return (peer.address != peer.successor[0]);

  }

  /**
   * Search the finger table and successor list for an optimal predecessor of id.
   * @param {Buffer} id -
   * @return {string}
   */
  *closestPrecedingNode (id) {

    // iterate finger table
    for (var i = FINGER_LENGTH - 1; i > -1; i--) {

      let fingeri = this.finger[i];

      // finger table is cold
      if (!isAddress(fingeri)) {
        continue;
      }

      // found a good finger
      if (isBetween(this.id, toSha1(fingeri), id)) {
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
   * Search the Chord overlay network for the successor of key.
   * @param {string} key -
   * @return {Promise}
   */
  async lookup (key) {

    // invalid request
    if (!_.isString(key)) {
      throw new Error('"key" argument must be string');
    }

    const id = toSha1(key);

    try {

      // find successor
      let response = await doRpc(this.address, 'lookup', { id });

      // return successor (implicit promise)
      return response.successor;

    } catch (e) {

      // bubble error
      throw e;

    }

  }

  /**
   * Join the Chord overlay network that a well known host is participating in.
   * @param {string} host -
   * @return {Promise}
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

    const id = toSha1(this.address);

    try {

      // stop maintenance while updating state
      maintenance.stop();

      // find successor
      let lookupResponse = await doRpc(host, 'lookup', { id });

      // immediate successor address
      let newSuccessor0 = lookupResponse.successor;

      // invalid successor response
      if (!isAddress(newSuccessor0)) {
        throw new Error();
      }

      // get predecessor and successor list
      let stateResponse = await doRpc(newSuccessor0, 'state', {
        predecessor: true,
        successor: true
      });

      // predecessor address
      let newPredecessor = stateResponse.predecessor;

      // invalid predecessor response
      if (!isAddress(newPredecessor)) {
        throw new Error();
      }

      // adopt predecessor
      this.predecessor = newPredecessor;

      // adopt successor list
      let newSuccessor = stateResponse.successor;
      newSuccessor = newSuccessor.slice(0, this.r - 1);
      newSuccessor.unshift(newSuccessor0);
      this.successor = newSuccessor;

      // return this (implicit promise)
      return this;

    } catch (e) {

      // bubble error
      throw e;

    } finally {

      // ensure maintenance resumes even on failure
      maintenance.start.call(this);

    }

  }

}

module.exports = {
  Peer
};
