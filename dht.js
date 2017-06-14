const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const buffer = require('buffer').Buffer;


/**
 * Return a new client/stub instance for the address
 * @param {String} address in compact IP-address/port notation
 * @return {Client} client
 */
function startRpcClient (address) {
  
  const PROTO_PATH = __dirname + '/drpc.proto';

  const drpc = grpc.load(PROTO_PATH).drpc;

  return new drpc(address, grpc.credentials.createInsecure());

}

/**
 * 
 * @param {Client} client
 * 
 */
function stopRpcClient (client) {

  grpc.closeClient(client);

}

function startRpcServer (address, api) {

  const PROTO_PATH = __dirname + '/drpc.proto';

  const drpc = grpc.load(PROTO_PATH).drpc;

  var server = new grpc.Server();
  
  server.addService(drpc.service, api);
  
  server.bind(address, grpc.ServerCredentials.createInsecure());

  server.start();

  console.log(`LISTEN ${address} (${toSha1(address).toString('hex')})`);

  return server;

}

function stopRpcServer(server, callback) {

  server.tryShutdown(callback);

}

/**
 * Return whether an address is valid
 * @param {String} address in compact IP-address/port notation
 * @return {Boolean} the result of validation
 */
function validateAddress (address) {

  address = (typeof address === 'string') ? address : '';

  var [ip4, port] = address.trim().split(':');

  return !!port && (1 <= parseInt(port) <= 65535) && ip.isV4Format(ip4);

}

/**
 * Return a buffer sha1 hash of supplied value
 * @param {_} value
 * @return {Buffer} the sha1 buffer
 */
function toSha1 (value) {
  
  return crypto.createHash('sha1').update(value).digest();

}

/**
 * Return a high-res time pair relative to arbitrary pair last
 * @param {Array} relative time pair
 * @return {Object} the high-res difference between now and then
 */
function getTime (relative) {

  var [s, n] = process.hrtime(relative);
  
  return {seconds: s, nanos: n};

}

/**
 * Return whether keyhash is in (lower, upper]
 * @param {EventEmitter} call Call object for the handler to process
 * @param {function(Error, feature)} callback Response callback
 */
function isBucket (el, lower, upper) {

  // lower before upper
  if (lower.compare(upper) < 0) {

    // lower before el AND el before/at upper
    return lower.compare(el) < 0 && el.compare(upper) <= 0;

  // upper before lower
  } else {

    // lower before el OR el before/at upper
    return lower.compare(el) < 0 || el.compare(upper) <= 0;

    // BUG
    // strange case when single node network as all key hashes map to that bucket
    // how do they migrate to correct bucket when it joins the network?

  }

}

/**
 *
 * @param {EventEmitter} call Call object for the handler to process
 *
 */
function handlePing (call, callback) {

  callback(null, call.request);

}

/**
 * lookup request handler. Gets a request with a ipHash, and responds with a
 * LookupResponse object indicating whether there is a bucket for that key.
 * @param {EventEmitter} call Call object for the handler to process
 * @param {function(Error, feature)} callback Response callback
 */
function handleLookup (call, callback) {

  var targetHash = call.request.hash;

  var addressHash = toSha1(this.address);

  var successorHash = toSha1(this.successorAddress);

  // BUG
  // strange case when target bucket is local one
  // has to make round trip

  // check whether key hash is in peer's bucket
  if (isBucket(targetHash, addressHash, successorHash)) {

    // notify client of the bucket address
    callback(null, { address: this.successorAddress });

  // forward the lookup to the peer
  } else {

    var peer = startRpcClient(this.successorAddress);

    // client will close connection
    peer.lookup(targetHash, callback);

  }

}

/**
 * 
 * @param {EventEmitter} call Call object for the handler to process
 * @param {function(Error, feature)} callback Response callback
 */
function handleGet (call, callback) {


  var keyHash = call.request.hash;

  // NOTE
  // always get because non destructive

  if (this.storage.hasOwnProperty(keyHash.toString('hex'))) {

    callback(null, { hash: keyHash, value: this.storage[keyHash.toString('hex')] });
  
  } else {
  
    callback(new Error('Invalid key'));
  
  }

}

/**
 * 
 * @param {EventEmitter} call Call object for the handler to process
 * @param {function(Error, feature)} callback Response callback
 */
function handleSet (call, callback) {

  var keyHash = call.request.hash;

  // TODO
  // only set if valid_keyhash for this bucket because destructive
  // but this comes at perf cost or storage cost for storing pred_peer
  // has to be some error to check

  this.storage[keyHash.toString('hex')] = call.request.value;
  
  callback(null, { hash: keyHash });

}

/**
 * private
 * @param {String} address 
 * @param {Buffer} hash
 * @param {function(Error, Object)} callback
 */
function findSuccessor (address, hash, cb) {

  if (!validateAddress(address)) {
    throw new Error('Invalid IP-address/port: ' + address);
  }

  if (typeof cb != 'function') cb = () => {};

  var peer = startRpcClient(address);

  peer.lookup({ hash }, (err, res) => {
    
    stopRpcClient(peer);
    
    cb(err, res);

  });

}

/**
 * 
 * @param {Number} port
 */
function Peer (port) {

  this.address = ip.address() + ':' + port;

  if (!validateAddress(this.address)) {
    throw new Error('Invalid IP-address/port');
  }

  // point to self
  this.successorAddress = this.address;

  this.server = startRpcServer(this.address, {
    ping: handlePing.bind(this),
    lookup: handleLookup.bind(this),
    get: handleGet.bind(this),
    set: handleSet.bind(this)
  });

  this.storage = {};

}

/**
 * 
 * @param {String} address
 */
Peer.prototype.ping = function (address, cb) {

  if (!validateAddress(address)) {
    throw new Error('Invalid IP-address/port: ' + address);
  }

  if (typeof cb != 'function') cb = () => {};

  var peer = startRpcClient(address);

  peer.ping(getTime(), (err, res) => {

      var timeDif = getTime([res.seconds, res.nanos]);

      console.log(`PING ${address} (${toSha1(address).toString('hex')}) ${(timeDif.nanos / 1e6).toPrecision(3)} ms`);

      stopRpcClient(peer);

      cb(err, res);


  });

}

/**
 * 
 * @param {String} peerAddress 
 */
Peer.prototype.join = function (address, cb) {

  if (!validateAddress(address)) {
    throw new Error('Invalid IP-address/port: ' + address);
  }

  if (typeof cb != 'function') cb = () => {};

  var peer = startRpcClient(address);

  var hash = toSha1(this.address);

  peer.lookup({ hash }, (err, res) => {

    if (err) {

      cb(err);
    
    } else {

      this.successorAddress = res.address;

      stopRpcClient(peer);

      cb(err, res);

    }

  });

}

/**
 * 
 * @param {Buffer} keyHash
 * @param {function(Error, Object)} callback
 */
Peer.prototype.get = function (key, cb) {

  var hash = toSha1(key);

  if (typeof cb != 'function') cb = () => {};

  findSuccessor(this.address, hash, (err, res) => {

    if (err) {
      
      cb(err);

    } else {

      var peer = startRpcClient(res.address);

      peer.get({ hash }, (err, res) => {
    
        stopRpcClient(peer);
        
        cb(err, res);

      });

    }

  });

}

/**
 * 
 * @param {Buffer} keyHash 
 * @param {Buffer} value
 * @param {function(Error)} callback
 */
Peer.prototype.set = function (key, val, cb) {

  var hash = toSha1(key);

  if (typeof cb != 'function') cb = () => {};

  findSuccessor(this.address, hash, (err, res) => {

    if (err) {
      
      cb(err);

    } else {

      var peer = startRpcClient(res.address);

      peer.set({ hash, value: buffer.from(val) }, (err, res) => {
    
        stopRpcClient(peer);
        
        cb(err, res);

      });

    }

  });

}

Peer.prototype.dump = function (cb) {

  if (typeof cb != 'function') cb = () => {};

  var address = this.address;
  var addressHash = toSha1(address).toString('hex');

  var successorAddress = this.successorAddress;
  var successorHash = toSha1(successorAddress).toString('hex');

  console.log(`SELF ${address} (${addressHash})\n`+
              `PEER ${successorAddress} (${successorHash})`);

  for (var prop in this.storage) {
    console.log(`DATA ${prop}: ${this.storage[prop]}`);
  }

  cb(null);

}

module.exports = {
  Peer
}
