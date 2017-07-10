'use strict'

const _ = require('underscore');
const Buffer = require('buffer').Buffer;
const getPort = require('get-port');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const endpoints = require('./chord_endpoints.js');
const utils = require('../lib/utils.js');
const isPort = utils.isPort;
const isAddress = utils.isAddress;
const isBetween = utils.isBetween;
const toSHA1 = utils.toSHA1;
const grpc = require('grpc');
const CHORD_PROTO = grpc.load(__dirname + '/chord.proto').CHORD_PROTO;
const doRPC = utils.doRPC(CHORD_PROTO);
const MIN_SUCCESSOR_LENGTH = 1 // default successor-list-length and replica-set-size
const FINGER_LENGTH = 160 // default finger-list length (should be 160 for sha1)
const FINGER_BASE = Array(FINGER_LENGTH).fill(0).map((_, i) => bignum.pow(2, i)); // powers of 2 closure
const MAX_ID = bignum.pow(2, 160); // 160 because using SHA1 hash function

/**
 * API for starting and stopping the maintenance protocol.
 */
const maintenance = (() => {

  // timeout closure
  var timeout = undefined;

  // return API
  return {

    // do not use arrow function so that context can be bound
    start: function () {
     timeout = setInterval(() => {
        fixPredecessor.call(this);
        fixFinger.call(this);
        fixSuccessor.call(this);
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

    if (!isAddress(this.predecessor)) {
      //throw new Error();
    } else {

      await doRPC(this.predecessor, 'ping', { 
        sender: this.address
      });

    }  

  } catch (e) {

    console.error('fixPredecessor', e);

    // TODO if error === Connect Failed
    this.predecessor = undefined;
    // else throw error

  }

  return;
  
}

/**
 * Maintain the finger table entries.
 */
const fixFinger = (() => {

  // finger index closure
  var i = 0;

  // do not use arrow function so that context can be bound
  return async function () {

    try {

      // generate finger id
      let id = bignum.fromBuffer(this.id)
                     .add(FINGER_BASE[i])
                     .mod(MAX_ID)
                     .toBuffer();

      // find successor
      let response = await doRPC(this.address, 'lookup', { id });

      let successor = response.successor;

      // update finger table entry
      this.finger[i] = successor;

    } catch (e) {

      console.error("fixFingers", e);

    }

    // increment finger index
    i += 1;

    // check maximum
    if (FINGER_LENGTH <= i) {
      i = 0;
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
      let stateResponse = await doRPC(successor0, 'state', {
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
        && isBetween(this.id, toSHA1(potentialSuccessor0), toSHA1(successor0))) {

        try {

          // re-fetch because immediate successor has changed
          stateResponse = await doRPC(potentialSuccessor0, 'state', {
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
    await doRPC(this.successor[0], 'notify', {
      sender: this.address
    });

  } catch (_) {

    // ignore failure

  }

  return;

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
  constructor (options) {

    super();

    options = options || {};

    _.defaults(options , {
      nSuccessors: MIN_SUCCESSOR_LENGTH
    });

    // predecessor address
    this.predecessor = undefined;

    // initialize successor list and finger table
    this.r = options.nSuccessors;
    this.successor = new Array(this.r);
    this.finger = new Array(FINGER_LENGTH);
    
    // instance gRPC server and register chord protocol
    this.server = new grpc.Server();
    this.server.addService(CHORD_PROTO.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));
    
  }

  /**
   * Start RPC server with all registered services, and start maintenance protocol.
   */
  async listen (port, host) {

    if (port != 0 && !isPort(port)) {
      throw new Error('"port" argument must be number between 0 and 65536');
    }

    // TODOs
    // - handle no host given

    try {
      port = await getPort(port);
    } catch (e) {
      throw e;
    }
    
    this.address = host + ':' + port;
    this.successor.fill(this.address);
    this.finger.fill(this.address);
    this.id = toSHA1(this.address);
    this.server.bind(this.address, grpc.ServerCredentials.createInsecure());
    this.server.start();
    maintenance.start.call(this);
    return this.address;

  }

  /**
   * Stop RPC server with all registered services, and stop maintenance protocol.
   */
  close () {
    
    maintenance.stop();
    this.server.forceShutdown();
    this.id = undefined;
    this.finger.fill(undefined);
    this.successor.fill(undefined);
    this.address = undefined;
    return;

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
   * Ping a remote peer.
   * @param {string} host -
   * @return {string}
   */
  async ping (host) {

    // invalid request
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    try {

      var t0 = process.hrtime();

      let response = await doRPC(host, 'ping', { 
        sender: this.address 
      });

      return process.hrtime(t0);

    } catch (e) {

      // bubble error
      throw e;

    }

  }

  /**
   * State info of remote peer.
   * @param {string} host -
   * @return {string}
   */
  async state (host, withFingers = false) {

    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    try {

      let response = await doRPC(host, 'state', {
        predecessor: true,
        successor: true,
        finger: withFingers
      });

      return response;

    } catch (e) {

      throw e

    }

  }

  /**
   * Join the Chord overlay network that host is participating in.
   * @param {string} host -
   * @return {Promise}
   */
  async join (host) {

    // invalid state
    if (Peer.isJoined(this)) {
      throw new Error('cannot join again');
    }

    // invalid request
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    // invalid request
    if (host === this.address) {
      throw new Error('cannot join to self');
    }

    try {

      // stop maintenance while updating state
      maintenance.stop();

      // find successor
      let lookupResponse = await doRPC(host, 'lookup', { id: toSHA1(this.address) });

      // immediate successor address
      let newSuccessor0 = lookupResponse.successor;

      // invalid successor response
      if (!isAddress(newSuccessor0)) {
        throw new Error();
      }

      // get predecessor and successor list
      let stateResponse = await doRPC(newSuccessor0, 'state', {
        predecessor: true,
        successor: true
      });

      // predecessor address
      let newPredecessor = stateResponse.predecessor

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

      // NOTEs
      // - "fully joined" only after the first notify event occurs
      // because fixSuccessor on predecessor has only then incorporated this node in ring.
      // - otherwise, bug occurs: partition before set and set before notify
      // - effect of bug: this node joins then sets its own id (or other owned key), set is then done on successor and not this!
      // - okay for set/get/del(lookup) to be called before stabilization, but not partition!
      // - bucket class calls partition on join
      this.once('notify', () => {
        this.emit('join', this.successor[0]);
      });

      return;

    } catch (e) {

      // bubble error
      throw e;

    } finally {

      // ensure maintenance resumes even on failure
      maintenance.start.call(this);

    }

  }

  /**
   * Search the Chord overlay network for the successor of key.
   * @param {string | Buffer} id -
   * @return {Promise}
   */
  async lookup (id) {

    // string to SHA1 buffer
    if (_.isString(id)) {
      id = toSHA1(id);
    }

    // invalid request
    if (!Buffer.isBuffer(id)) {
      throw new Error('"id" argument must be string or buffer');
    }

    try {

      // find successor
      let response = await doRPC(this.address, 'lookup', { id });

      // return successor (implicit promise)
      return response.successor;

    } catch (e) {

      // bubble error
      throw e;

    }

  }

}
module.exports = { Peer }
