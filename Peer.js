'use strict';

const _ = require('underscore');
const ip = require('ip');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const grpc = require('grpc');
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;
const endpoints = require('./endpoints.js');
const util = require('./util.js');
const isPort = util.isPort;
const isAddress = util.isAddress;
const isBetween = util.isBetween;
const toSha1 = util.toSha1;
const doRpc = util.doRpc;

/**
 * DEFINE PEER CONSTANTS
 */
const MAX_ID = bignum.pow(2, 160); // 160 because using SHA1 hash function

/**
 * DEFINE PEER CLASS
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port, m, r) {

    super();

    // TODO
    // - use random port when port is 0

    if (!isPort(port)) {
      throw new Error('"port" argument must be number between 1 and 65536');
    }

    // set address
    this.address = ip.address() + ':' + port;

    // initialize node identifier
    this.id = toSha1(this.address);

    // predecessor address
    this.predecessor = undefined;

    // initialize finger table
    this.m = m; // finger-list-length
    this.finger = new Array(m);
    this.finger.fill('');
    this.iFinger = 0;

    // initialize powers of 2
    this.fingerBase = new Array(m);
    this.fingerBase.fill(undefined);
    this.fingerBase = this.fingerBase.map((_,i) => bignum.pow(2, i));

    // initialize successor list
    this.r = r; // successor-list-length (also replica-set-size)
    this.successor = new Array(r);
    this.successor.fill(this.address);

    // local bucket storage
    this.bucket = {};

    this.server = new grpc.Server();
    
    // bind endpoints to peer context and give to gRPC server
    this.server.addService(chordRPC.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));
    
    this.server.bind(this.address, grpc.ServerCredentials.createInsecure());

    this.server.start();

    // periodic 
    this.timeout = setInterval(() => {

      checkPredecessor.call(this);
      
      fixFingers.call(this);
      
      stabilize.call(this);

    }, 1000);

  }

  static isPeer (object) {

    return (object instanceof Peer);

  }

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
  *closestPrecedingFinger (id) {

    // iterate finger table
    for (var i = this.m - 1; i > -1; i--) {

      let fingerAddress = this.finger[i];

      // finger table is cold
      if (!isAddress(fingerAddress)) {
        continue;
      }

      let fingerId = toSha1(fingerAddress);

      // found a good finger
      if (isBetween(fingerId, this.id, id)) {
        yield fingerAddress;
      }

    }

    // no finger is applicable so default to successor list
    // NOTE
    // - in the case where we are called from stabilize, and list length is 1, and the list is empty
    // - and all fingers are bad, we skip this loop and return address of self
    for (let successor of this.successor) {

      if (!isAddress(successor)) {
        continue;
      }

      yield successor;

    }

    // impossible case as successor[0] *must* always be defined unless error
    yield this.address;

  }

}

/**
 * EXPOSE PEER CLASS
 */
module.exports = Peer;

/**
 * 
 */
async function checkPredecessor () {

  try {

    let pingRequest = { 
      sender: {
        ipv4: this.address
      }
    };

    if (isAddress(this.predecessor)) {
      await doRpc(this.predecessor, 'ping', pingRequest);
    }

  } catch (e) {

    console.error('checkPredecessor', e);

    // TODO if error === Connect Failed
    this.predecessor = undefined;
    // else throw error

  }
  
}

/**
 *
 */
async function fixFingers () {

  try {

    // generate finger id (this.id + 2^i % 2^160)
    let id = bignum.fromBuffer(this.id)
                   .add(this.fingerBase[this.iFinger])
                   .mod(MAX_ID)
                   .toBuffer();

    let lookupRequest = { id };

    // find the successor for id
    let lookupResponse = await doRpc(this.address, 'lookup', lookupRequest);

    let successor0 = lookupResponse.successor.ipv4;

    // invalid response
    if (!isAddress(successor0)) {
      throw new Error();
    }

    // update finger table entry
    this.finger[this.iFinger] = successor0;

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

  var done = false;

  var i = 0;

  // define immediate successor
  var successor0 = this.successor[i];

  // invalid state
  if (!isAddress(successor0)) {
    throw new Error('invalid -- no immediate successor');
  }

  // NOTEs
  // - see Zave 2010 StabilizeFromSuccessor step
  // - every doRPC is also a check for liveliness
  do {
  
    try {

      // fetch immediate successor's predecessor and successor list (potentially the new successor)
      let infoResponse = await doRpc(successor0, 'info');

      // update the successor list
      let updatedSuccessor = _.pluck(infoResponse.successor, 'ipv4');
      updatedSuccessor = updatedSuccessor.slice(0, this.r - 1);
      updatedSuccessor.unshift(successor0);

      // predecessor address
      let potentialSuccessor0 = infoResponse.predecessor.ipv4;

      // NOTEs
      // - see Zave 2010 StabilizeFromPredecessor step
      // - true when potentialSuccessor0 just joined
      if (isAddress(potentialSuccessor0)
        && isBetween(toSha1(potentialSuccessor0), this.id, toSha1(successor0))) {

          // re-fetch because immediate successor changed and knows most up to date successor list
          infoResponse = await doRpc(potentialSuccessor0, 'info');

          // re-update the successor list
          updatedSuccessor = _.pluck(infoResponse.successor, 'ipv4');
          updatedSuccessor = updatedSuccessor.slice(0, this.r - 1);
          updatedSuccessor.unshift(potentialSuccessor0);

      }

      let up = _.chain(updatedSuccessor).difference(this.successor).without(this.address).value();

      let down = _.chain(this.successor).difference(updatedSuccessor).without(this.address).value();

      this.successor = updatedSuccessor;

      done = true;

      if (!_.isEmpty(up)) {
        this.emit('successor::up', up);
      }

      if (!_.isEmpty(down)) {
        this.emit('successor::down', down);
      }

    // immediate successor has died
    } catch (e) {

      console.error('stabilize', e);

      i++;

      successor0 = this.successor[i];

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
    this.successor.fill(this.address);
  }

  let notifyRequest = {
    sender: {
      ipv4: this.address
    }
  };

  // notify immediate successor (use list value - may be updated) about this node (refresh its predecessor)
  await doRpc(this.successor[0], 'notify', notifyRequest);

}
