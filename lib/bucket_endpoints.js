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

  var predecessor = this.predecessor

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (!isBetween(toSHA1(predecessor), id, this.id)
    && id.compare(this.id) != 0) {

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

  var predecessor = this.predecessor

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (!isBetween(toSHA1(predecessor), id, this.id)
    && id.compare(this.id) != 0) {

    // invalid request
    return cb(new Error('100')); 

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

  var predecessor = this.predecessor

  // invalid predecessor state
  if (!isAddress(predecessor)) {
    return cb(new Error());
  }

  // check if key belongs to this node
  if (!isBetween(toSHA1(predecessor), id, this.id)
    && id.compare(this.id) != 0) {

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

/**
 * @callback allCallback
 * @param {Error}
 * @param {object} 
 */

/**
 * Respond to all type RPC
 * @param {object} call
 * @param {delCallback} cb
 */
function all (call, cb) {

  var sender = call.request.sender

  // invalid all request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  var contents = []

  for (let key in this.hashtable) {
    contents.push({ 
      id: Buffer.from(key, 'hex'),
      value: this.hashtable[key]
    });
  }

  this.emit('all', sender);

  cb(null, { contents });

}

module.exports = { get, set, del, all }
