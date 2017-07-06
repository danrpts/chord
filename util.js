'use strict';

const _ = require('underscore');
const ip = require('ip');
const crypto = require('crypto');
const grpc = require('grpc');

/**
 *
 */
const CHORD_PROTO_PATH = '/chord.proto';
const CHORD_PROTO_GRPC = grpc.load(__dirname + CHORD_PROTO_PATH).CHORD_PROTO;


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
function isBetween (lower, element, upper) {

  // think about own case when lower === upper

  // lower before upper
  if (lower.compare(upper) < 0) {

    // lower before element AND element before upper
    return (lower.compare(element) < 0 && element.compare(upper) < 0);

  // upper before lower or equal
  } else {

    // lower before element OR el before upper
    return (lower.compare(element) < 0) || (element.compare(upper) < 0);

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

  var client = new CHORD_PROTO_GRPC(receiver, grpc.credentials.createInsecure());

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