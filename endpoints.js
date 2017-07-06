'use strict';

const _ = require('underscore');
const Buffer = require('buffer').Buffer;
const util = require('./util.js');
const isPort = util.isPort;
const isAddress = util.isAddress;
const isBetween = util.isBetween;
const toSha1 = util.toSha1;
const doRpc = util.doRpc;

/**
 *
 */
function ping (call, cb) {

  var sender = call.request.sender;

  // bad request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  this.emit('ping', call.request);

  cb(null, call.request);

}

/**
 *
 */
function state (call, cb) {

  var response = {};

  if (call.request.predecessor) {
    response.predecessor = isAddress(this.predecessor) ? this.predecessor : '';
  }

  if (call.request.successor) {
    response.successor = _.map(this.successor, successori => {
      return isAddress(successori) ? successori : ''
    });
  }

  if (call.request.finger) {
    response.finger = _.map(this.finger, fingeri => {
      return isAddress(fingeri) ? fingeri : '';
    });
  }

  this.emit('state', call.request, response);

  cb(null, response);

}

/**
 * see Stoica et al. 2001 findSuccessor
 */
async function lookup (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  // optimization (lookup called on self)
  if (this.id.compare(keyId) === 0) {

    let response = {
      successor: isAddress(this.address) ? this.address : ''
    };

    this.emit('lookup', call.request, response);

    return cb(null, response);
  
  }

  // TODO
  // - test the catch and continue mechanism by simulating successor failure
  for (let predecessori of this.closestPrecedingNode(keyId)) {

    try {

      // check alive
      await doRpc(predecessori, 'ping', {
        sender: isAddress(this.address) ? this.address : ''
      });

      let predecessoriId = toSha1(predecessori);

      // base case -- successor found
      if (predecessoriId.compare(keyId) === 0
        || isBetween(this.id, keyId, predecessoriId)) {

        let response = {
          successor: isAddress(predecessori) ? predecessori : ''
        };

        // emit lookup was successful on this peer
        this.emit('lookup', call.request, response);
        
        // bubble response
        return cb(null, response);

      // recursive case -- forward lookup to closest peer
      } else {

        //console.log(`lookup calling... ${closestPrecedingNodeAddress}`);

        // check alive and call lookup
        let response = await doRpc(predecessori, 'lookup', { id: keyId });

        // bubble response
        return cb(null, response);

      }

    // successor is dead (grab next live successor from finger table or successor list)
    } catch (e) {

      // NOTE
      // - fixFingers will refresh stale entries
      // - stabilize will rectify dead successors

      //console.error(`${predecessori} dead -- lookup rerouting`, e);

      // if error === Connect Failed or Timeout
      
      // try next
      continue;

      // else throw exception

    }

  }

}

/**
 * see Zave 2010 rectify
 */
async function notify (call, cb) {

  // notifier's address
  let sender = call.request.sender;

  // invalid request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  // if no predecessor set or is closer predecessor than the current predecessor
  if (!isAddress(this.predecessor)
    || isBetween(toSha1(this.predecessor), toSha1(sender), this.id)) {

    // update current predecessor
    this.predecessor = sender;

  } else {

    /* current predecessor is defined */

    try {

      await doRpc(this.predecessor, 'ping', {
        sender: isAddress(this.address) ? this.address : ''
      });

      /* current predecessor is alive and valid */

    // current predecessor has died
    } catch (e) {

     // console.error('onNotifyRequest', e);
    
      // TODO if error is Connect Failed or Timeout
      this.predecessor = sender;
      // else cb(new Error());

    }
  
  }

  this.emit('notify', call.request);

  cb(null);

}

/**
 * EXPOSE PEER SERVER API
 */
module.exports = {
  ping,
  state,
  lookup, // aka findSuccessor
  notify
}
