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

  var sender = call.request.sender.ipv4;

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
function echo (call, cb) {

  var sender = call.request.sender.ipv4;

  // bad request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  // TODOs
  // - reject echo from nodes in other networks

  this.emit('echo', call.request);

  cb(null);

}
/**
 *
 */
async function lookup (call, cb) {

  var id = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  // optimization
  if (this.id.compare(id) === 0) {

    let lookupResponse = {
      successor: {
        ipv4: this.address
      }
    };

    return cb(null, lookupResponse);

  }

  var pingRequest = {
    sender: {
      ipv4: this.address
    }
  };

  var lookupRequest = { id };

  // TODO
  // - test the catch and continue mechanism by simulating successor failure
  for (let closest of this.closestPrecedingFinger(id)) {

    try {

      // found successor
      if (isBetween(id, this.id, toSha1(closest), false, true)) {

        // check alive
        await doRpc(closest, 'ping', pingRequest);

        // break from outer loop

        let lookupResponse = {
          successor: {
            ipv4: closest
          }
        };
                  
        return cb(null, lookupResponse);

      // forward to first live successor
      } else {

        //console.log(`lookup calling... ${closestPrecedingNodeAddress}`);

        // check alive and forward
        let lookupResponse = await doRpc(closest, 'lookup', lookupRequest);

        let successor0 = lookupResponse.successor.ipv4;

        // invalid response
        if (!isAddress(successor0)) {
          // TODOs
          // - handle this case (kill that successor?)
          //return new Error();
        }

        // break from outer loop
        return cb(null, lookupResponse);

      }

    // successor is dead (grab next live successor from finger table or successor list)
    } catch (e) {

      // NOTE
      // - stabilize will clean dead successor up

      console.error('lookup rerouting', e);

      // if error === Connect Failed or Timeout
      // try next successor
      continue;

      // else throw exception

    }

  }

}

/**
 *
 */
async function info (call, cb) {

  var infoResponse = {
    predecessor: {
      ipv4: this.predecessor || '',
    },
    successor: _.map(this.successor, address => {
      return {
        ipv4: address
      }
    })
  };

  cb(null, infoResponse);

}

/**
 * Get and delete all entries belonging to requesting node
 */
function split (call, cb) {

  var id = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  var partition = [];

  var selfId = this.id;

  for (let key in this.bucket) {

    let keyId = Buffer.from(key, 'hex');

    if (isBetween(id, keyId, selfId, true, false)) {

      let value = Buffer.from(this.bucket[key]);

      partition.push({ id: keyId, value });

      delete this.bucket[key];

    }

  }

  this.emit('split');

  cb(null, { partition });

}

/**
 * see Zave 2010 Rectify step
 */
async function notify (call, cb) {

  // notifier's address
  let sender = call.request.sender.ipv4;

  // bad request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  // if no predecessor set or is closer predecessor than the current predecessor
  if (!isAddress(this.predecessor)
  || isBetween(toSha1(sender), toSha1(this.predecessor), this.id)) {

    // update currant predecessor
    this.predecessor = sender;

  } else {

    /* current predecessor is defined */

    try {

      let pingRequest = {
        sender: {
          ipv4: this.address
        }
      };

      await doRpc(this.predecessor, 'ping', pingRequest);

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
function get (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  var predecessorId = toSha1(predecessor);

  var selfId = this.id;

  // check if key belongs to this node
  if (!isBetween(keyId, predecessorId, selfId, false, true)) {

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
function set (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  var predecessorId = toSha1(predecessor);

  var selfId = this.id;

  // check if key belongs to this node
  if (!isBetween(keyId, predecessorId, selfId, false, true)) {

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
function del (call, cb) {

  var keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  var predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  var predecessorId = toSha1(predecessor);

  var selfId = this.id;

  // check if key belongs to this node
  if (!isBetween(keyId, predecessorId, selfId, false, true)) {

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
 * EXPOSE PEER SERVER API
 */
module.exports = {
  ping,
  echo,
  lookup, // aka findSuccessor
  info,
  split,
  notify,
  get,
  set,
  delete: del,
}
