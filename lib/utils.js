'use strict';

const _ = require('underscore');
const net = require('net');
const crypto = require('crypto');
const grpc = require('grpc');

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

  const [ipv4, port] = address.trim().split(':');

  return isPort(parseInt(port)) && net.isIPv4(ipv4);

}

/**
 *
 */
function isBetween (lower, element, upper) {

  // think about own case when lower === upper

  // lower less than upper
  if (lower.compare(upper) < 0) {

    // lower less than element AND element less than upper
    return (lower.compare(element) < 0 && element.compare(upper) < 0);

  // upper less than lower or equal
  } else {

    // lower less than element OR element less than upper
    return (lower.compare(element) < 0) || (element.compare(upper) < 0);

  }

}

/**
 *
 */
function toSHA1 (value) {
  
  return crypto.createHash('sha1').update(value).digest();

}

/**
 *
 */
const doRPC = GRPC => function(host, method, request) {

  /* Note: GRPC is curried */

  if (!isAddress(host)) {
    throw new Error('"host" argument must be compact IP-address:port');
  }

  request = request || {};

  var client = new GRPC(host, grpc.credentials.createInsecure());

  return new Promise((resolve, reject) => {

    client[method](request, (error, response) => {

      if (error) reject(error);

      else resolve(response);

      grpc.closeClient(client);

    });

  });

}

/**
 *
 */
function toStateString (object, withFingers = false) {

  var info = '';
  info += `<Predecessor> ${(object.predecessor) ? object.predecessor : ''}\n`;
  info += `<Self> ${object.address}\n`;
  
  var successor = object.successor;
  for (let i in successor) {
    info += `<Successor ${i}> ${successor[i]}\n`;
  }

  if (withFingers) {
    let finger = object.finger
    for (let i in finger) {
      info += `<Finger ${i}> ${finger[i]}\n`;
    }
  }

  return info;

}

module.exports = { isPort, isAddress, isBetween, toSHA1, doRPC, toStateString }
