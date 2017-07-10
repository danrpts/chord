'use strict'

const _ = require('underscore');
const Buffer = require('buffer').Buffer
const utils = require('./utils.js');
const isAddress = utils.isAddress
const isBetween = utils.isBetween
const toSHA1 = utils.toSHA1
const grpc = require('grpc');
const CHORD_PROTO = grpc.load(__dirname + '/chord.proto').CHORD_PROTO
const doRPC = utils.doRPC(CHORD_PROTO);

/**
 * Search the finger table and successor list for an optimal predecessor of id.
 * @param {Buffer} id -
 * @return {string}
 */
function* route (id) {

  /* see Stoica et al. 2001 and Nowell et al. 2002 closestPrecedingNode */

  if (!Buffer.isBuffer(id)) {
    throw new Error();
  }

  for (let i = this.finger.length - 1; i > -1; i--) {
    
    let finger = this.finger[i];

    if (isBetween(this.id, toSHA1(finger), id)) {
      yield finger;
    }

  }

  // fallback to successor
  return this.successor[0];
      
}

/**
 * @callback pingCallback
 * @param {Error}
 * @param {object} 
 */

/**
 * Respond to ping type RPC
 * @param {object} call
 * @param {pingCallback} cb
 */
function ping (call, cb) {

  var sender = call.request.sender

  // bad request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  this.emit('ping', sender);

  cb(null, call.request);

}

/**
 * @callback stateCallback
 * @param {Error}
 * @param {object} 
 */

/**
 * Respond to state type RPC
 * @param {object} call
 * @param {stateCallback} cb
 */
function state (call, cb) {

  const response = {};

  if (call.request.predecessor) {
    response.predecessor = isAddress(this.predecessor) ? this.predecessor : ''
  }

  if (call.request.successor) {
    response.successor = _.map(this.successor, successor => {
      return isAddress(successor) ? successor : ''
    });
  }

  if (call.request.finger) {
    response.finger = _.map(this.finger, finger => {
      return isAddress(finger) ? finger : ''
    });
  }

  cb(null, response);

}

/**
 * @callback lookupCallback
 * @param {Error}
 * @param {object} 
 */

/**
 * Respond to a lookup type RPC
 * @param {object} call
 * @param {lookupCallback} cb
 */
async function lookup (call, cb) {

  /* see Stoica et al. 2001 findSuccessor */

  const id = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  const successor = this.successor[0];
  const successorId = toSHA1(successor);

  // base case
  if (isBetween(this.id, id, successorId)
    || id.compare(successorId) === 0) {

    try {

      await doRPC(successor, 'ping', { sender: this.address });

      return cb(null, { successor });

    } catch (e) {}

  }

  // recursive case
  try {

      const response = await doRPC(successor, 'lookup', { id });

      return cb(null, { successor: response.successor });

    } catch (e) {

      console.error('dead successor', e);
      process.exit(1);

  }

  // // recursive case (optimized)
  // for (let hop of route.call(this, id)) {

  //   try {

  //     const response = await doRPC(hop, 'lookup', { id });

  //     return cb(null, { successor: response.successor });

  //   } catch (e) {

  //     console.error('dead hop', e);
  //     process.exit(1);

  //   }

  // }

}

/**
 * @callback notifyCallback
 * @param {Error}
 * @param {object} 
 */

/**
 * Respond to a notify RPC
 * @param {object} call
 * @param {notifyCallback} cb
 */
async function notify (call, cb) {

  /* see Stoica et. al 2001 notify and Zave 2010 rectify */

  // notifier's address
  let sender = call.request.sender

  // invalid request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  // if no predecessor set or is closer predecessor than the current predecessor
  if (!isAddress(this.predecessor)
    || isBetween(toSHA1(this.predecessor), toSHA1(sender), this.id)) {

    // update current predecessor
    this.predecessor = sender

  } else {

    /* current predecessor is defined */

    try {

      await doRPC(this.predecessor, 'ping', {
        sender: isAddress(this.address) ? this.address : ''
      });

      /* current predecessor is alive and valid */

    // current predecessor has died
    } catch (e) {

     // console.error('onNotifyRequest', e);
    
      // TODO if error is Connect Failed or Timeout
      this.predecessor = sender
      // else cb(new Error());

    }
  
  }

  this.emit('notify', sender);

  cb(null);

}

module.exports = { ping, state, lookup, notify }
