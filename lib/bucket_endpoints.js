'use strict'

const _ = require('underscore');
const Buffer = require('buffer').Buffer
const utils = require('./utils.js');
const isAddress = utils.isAddress
const isBetween = utils.isBetween
const toSHA1 = utils.toSHA1

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
function get (call, cb) {

  var id = call.request.id

  // invalid get request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  var successor0 = this.successor[0]

  // invalid immediate successor state
  if (!isAddress(successor0)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (this.id.compare(id) != 0
    && !isBetween(this.id, id, toSHA1(successor0))) {

    // invalid request
    return cb(new Error()); 

  }

  var key = id.toString('hex');

  if (!_.has(this.hashtable, key)) {

    // invalid request
    return cb(new Error()); 
  }

  this.emit('get', key);

  cb(null, { value: this.hashtable[key] });

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
function set (call, cb) {

  var id = call.request.id

  // invalid request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  var successor0 = this.successor[0]

  // invalid immediate successor state
  if (!isAddress(successor0)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (this.id.compare(id) != 0
    && !isBetween(this.id, id, toSHA1(successor0))) {

    // invalid request
    return cb(new Error()); 

  }

  var key = id.toString('hex');

  this.hashtable[key] = call.request.value

  this.emit('set', key);

  cb(null);

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
function del (call, cb) {

  var id = call.request.id

  // invalid request
  if (!Buffer.isBuffer(id)) {
    return cb(new Error());
  }

  var successor0 = this.successor[0];

  // invalid immediate successor state
  if (!isAddress(successor0)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (this.id.compare(id) != 0
    && !isBetween(this.id, id, toSHA1(successor0))) {

    // invalid request
    return cb(new Error()); 

  }

  var key = id.toString('hex');

  if (!_.has(this.hashtable, key)) {

    // invalid request
    return cb(new Error()); 
  }

  delete this.hashtable[key];

  this.emit('delete', key);

  cb(null);

}

module.exports = { get, set, del }
