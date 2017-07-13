const _ = require('underscore');
const Buffer = require('buffer').Buffer;
const utils = require('./utils.js');

const isAddress = utils.isAddress;
const isBetween = utils.isBetween;
const isStrictlyBetween = utils.isStrictlyBetween;
const toSHA1 = utils.toSHA1;

/**
 * @callback getCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to get type RPC
 * @param {object} call
 * @param {getCallback} cb
 */
function get(call, cb) {
  const keyId = call.request.id;

  // invalid get request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  const predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  const predecessorId = toSHA1(this.predecessor);

  // key is not between predecessor and this node,
  // and key is not this node,
  // and predecessor is not this node (node not singleton)
  if (!isBetween(predecessorId, keyId, this.id) && keyId.compare(this.id) !== 0) {
    return cb(new Error('invalid state for get request'));
  }

  const key = keyId.toString('hex');

  if (!_.has(this.hashtable, key)) {
    // invalid request
    return cb(new Error('invalid key for get request'));
  }

  this.emit('get', key);

  return cb(null, { value: this.hashtable[key] });
}

/**
 * @callback setCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to set type RPC
 * @param {object} call
 * @param {setCallback} cb
 */
function set(call, cb) {
  const keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  const predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  const predecessorId = toSHA1(this.predecessor);

  // key is not between predecessor and this node,
  // and key is not this node,
  // and predecessor is not this node (node not singleton)
  if (!isBetween(predecessorId, keyId, this.id) && keyId.compare(this.id) !== 0) {
    return cb(new Error('invalid state for set request'));
  }

  const key = keyId.toString('hex');

  this.hashtable[key] = call.request.value;

  this.emit('set', key);

  return cb(null);
}

/**
 * @callback delCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to delete type RPC
 * @param {object} call
 * @param {delCallback} cb
 */
function del(call, cb) {
  const keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  const predecessor = this.predecessor;

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  const predecessorId = toSHA1(this.predecessor);

  // key is not between predecessor and this node,
  // and key is not this node,
  // and predecessor is not this node (node not singleton)
  if (!isBetween(predecessorId, keyId, this.id) && keyId.compare(this.id) !== 0) {
    return cb(new Error('invalid state for delete request'));
  }

  const key = keyId.toString('hex');

  if (!_.has(this.hashtable, key)) {
    // invalid request
    return cb(new Error());
  }

  delete this.hashtable[key];

  this.emit('del', key);

  return cb(null);
}

/**
 * @callback partitionCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to all partition RPC
 * @param {object} call
 * @param {partitionCallback} cb
 */
function partition(call, cb) {
  const bound = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(bound)) {
    return cb(new Error());
  }

  const entries = [];

  Object.keys(this.hashtable).forEach((key) => {
    const keyId = Buffer.from(key, 'hex');

    // // edge case
    // if (keyId.compare(this.id) === 0) {
    //   return;
    // }

    // key is equal to upper bound, or
    if (keyId.compare(bound) === 0

      // bound is strictly between key and this node, and
      // key does not equal this node, or
      || isStrictlyBetween(keyId, bound, this.id)

      // upper bound is equal to this node (get all)
      || bound.compare(this.id) === 0) {
      entries.push({
        id: keyId,
        value: this.hashtable[key],
      });

      if (call.request.delete) {
        delete this.hashtable[key];
      }
    }
  });

  return cb(null, { entries });
}

module.exports = {
  get,
  set,
  del,
  partition,
};
