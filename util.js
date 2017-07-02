'use strict';

const _ = require('underscore');
const ip = require('ip');
const crypto = require('crypto');
const grpc = require('grpc');
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

/**
 *
 */
function isPort (port) {
    
  return _.isNumber(port) && (1 <= port) && (port <= 65536);

}

/**
 *
 */
function isAddress (address) {

  if (!_.isString(address)) {
    return false;
  }

  const [ip4, port] = address.trim().split(':');

  return isPort(parseInt(port)) && ip.isV4Format(ip4);

}

/**
 *
 */
function isBetween (el, lwr, upr, lwrIcl = false, uprIcl = false) {

  // lower before upper
  if (lwr.compare(upr) < 0) {

    // lower before el AND el before/at upper
    return (lwr.compare(el) < 0 && el.compare(upr) < 0) 
        || (lwrIcl && lwr.compare(el) === 0) 
        || (uprIcl && el.compare(upr) === 0);

  // upper before lower
  } else {

    // lower before el OR el before/at upper
    return (lwr.compare(el) < 0) || (el.compare(upr) < 0) 
        || (lwrIcl && lwr.compare(el) === 0) 
        || (uprIcl && el.compare(upr) === 0);

  }

}

/**
 *
 */
function toSha1 (value) {
  
  return crypto.createHash('sha1').update(value).digest();

}

/**
 *
 */
function doRpc (receiver, method, request) {

  // if (!Peer.isAddress(receiver)) {
  //   throw new Error('"receiver" argument must be compact IP-address:port');
  // }

  // TODO
  // if (!Peer.isMethod(method)) {
  //   throw new Error('"method" argument must be valid RPC method');
  // }

  request = request || {};

  var client = new chordRPC(receiver, grpc.credentials.createInsecure());

  return new Promise((resolve, reject) => {

    client[method](request, (error, response) => {

      if (error) reject(error);

      else resolve(response);

      grpc.closeClient(client);

    });

  });

}

module.exports = {
  isPort,
  isAddress,
  isBetween,
  toSha1,
  doRpc
}