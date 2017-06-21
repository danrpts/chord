const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const EventEmitter = require('events').EventEmitter;
const Buffer = require('buffer').Buffer;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

/**
 *
 */
function isAddress (addr) {

  addr = (typeof addr === 'string') ? addr : '';

  var [ip4, port] = addr.trim().split(':');

  port = parseInt(port);

  return !!port && 1024 <= port && port <= 65536 && ip.isV4Format(ip4);

}

/**
 *
 */
function call (addr, method, req) {

  if (!isAddress(addr)) {
    return new Error('"addr" argument must be compact IP-address:port');
  }

  req = req || {};

  var client = new chordRPC(addr, grpc.credentials.createInsecure());

  return new Promise((resolve, reject) => {

    client[method](req, (err, res) => {

      if (err) reject(err);

      else resolve(res);

      grpc.closeClient(client);

    });

  });

}

/**
 *
 */
function listen (addr, api) {

  if (!isAddress(addr)) {
    return new Error('"addr" argument must be compact IP-address:port');
  }

  var server = new grpc.Server();
  
  server.addService(chordRPC.service, api);
  
  server.bind(addr, grpc.ServerCredentials.createInsecure());

  server.start();

  return server;

}

/**
 *
 */
function toSha1 (val) {
  
  return crypto.createHash('sha1').update(val).digest();

}

/**
 *
 */
function between (el, lwr, upr, lwrIcl = false, uprIcl = false) {

  // lower before upper
  if (lwr.compare(upr) < 0) {

    // lower before hash AND hash before/at upper
    return (lwr.compare(el) < 0 && el.compare(upr) < 0) 
        || (lwrIcl && lwr.compare(el) === 0) 
        || (uprIcl && el.compare(upr) === 0);

  // upper before lower
  } else {

    // lower before hash OR hash before/at upper
    return (lwr.compare(el) < 0) || (el.compare(upr) < 0) 
        || (lwrIcl && lwr.compare(el) === 0) 
        || (uprIcl && el.compare(upr) === 0);

  }

}

/**
 *
 */
function onEchoPeer (call, cb) {

  this.emit('echo', call.request);

  cb(null);

}

/**
 *
 */
function onPingPeer (call, cb) {

  cb(null, call.request);

}

/**
 *
 */
function onGetPredecessor (call, cb) {

  var res = {};

  if (this.pAddr != null) {
    res.addr = this.pAddr;
  }

  cb(null, res);

}

/**
 *
 */
function onGetSuccessor (call, cb) {

  var res = {};

  if (this.sAddr != null) {
    res.addr = this.sAddr;
  }

  cb(null, res);

}

/**
 *
 */
async function onFindSuccessor (call, cb) {

  var hash = call.request.hash;

  // BUG
  // strange case when target bucket is local one
  // has to make round trip

  // check whether key hash is in peer's bucket
  if (between(hash, this._idHash, toSha1(this.sAddr), false, true)) {

    // notify client of the bucket address
    cb(null, { addr: this.sAddr });

  // forward the lookup to the peer
  } else {

    try {

     var res = await call(this.sAddr, 'findSuccessor', { hash });

     cb(null, res);

    } catch (err) {
    
      cb(err);

    }

  }

}

/**
 *
 */
function onNotifySuccessor (call, cb) {

  var addr = call.request.addr;

  if (this.pAddr === null || between(toSha1(addr), toSha1(this.pAddr), this._idHash)) {
    this.pAddr = addr;
  }

  cb(null);

}

/**
 *
 */
function onGetKey (call, cb) {

  var hash = call.request.hash;

  var hashStr = hash.toString('hex');

  if (this.storage.hasOwnProperty(hashStr)) {

    cb(null, { hash, val: this.storage[hashStr]}); 

  } else {

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onSetKey (call, cb) {

  var hash = call.request.hash;
  var hashStr = hash.toString('hex');
  var val = call.request.val;

  this.storage[hashStr] = val;

  // TODO
  // handle some error here

  cb(null, { hash, val}); 

}

/**
 *
 */
function onMovekey (call, cb) {

  var hashStr = call.request.hash.toString('hex');

  this.storage[hashStr] = call.request.val;

  cb(null, call.request); 

}

class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port) {

    super();

    this.addr = ip.address() + ':' + port;

    this._idHash = toSha1(this.addr);
    
    this.id = this._idHash.toString('hex');

    this.pAddr = null; // predecessor address
    
    this.sAddr = this.addr; // successor address

    this.server = listen(this.addr, {

      echoPeer: onEchoPeer.bind(this),
    
      pingPeer: onPingPeer.bind(this),
    
      getPredecessor: onGetPredecessor.bind(this),
    
      getSuccessor: onGetPredecessor.bind(this),
    
      findSuccessor: onFindSuccessor.bind(this),
    
      notifySuccessor: onNotifySuccessor.bind(this),
    
      getKey: onGetKey.bind(this),

      setKey: onSetKey.bind(this),

      moveKey: onMovekey.bind(this)
    
    });

    // TODO NOTE
    // in constructor for a reason
    this.timeout = setInterval(this.stabilize.bind(this), 5000);

    this.storage = {};

  }

  /**
   *
   */
  async echo (addr, msg) {

    var res;

    try {

      res = await call(addr, 'echoPeer', { msg, addr: this.addr });

    } catch (err) {

      console.log('echoPeer', err);

    }

    return res;

  }

  /**
   *
   */
  async ping (addr) {

    var res;

    function hrtimeObj (rel) {

      var [secs, nans] = process.hrtime(rel);
  
      return { secs, nans };

    }

    try {

      res = await call(addr, 'pingPeer', hrtimeObj());

      res.dif = hrtimeObj([res.secs, res.nans]);

    } catch (err) {

      console.log('pingPeer', err);

    }

    return res;

  }

  /**
   *
   */
  async join (addr) {

    var res;

    try {

      res = await call(addr, 'findSuccessor', { hash: this._idHash });

      this.pAddr = null;
      
      this.sAddr = res.addr;
    
    } catch (err) {
    
      console.log("findSuccessor", err);

    }

    return res;

  }

  /**
   *
   */
  async stabilize () {

    var res;

    try {

      // update successor
      res = await call(this.sAddr, 'getPredecessor');

      if (res.addr != '' && between(toSha1(res.addr), this._idHash, toSha1(this.sAddr))) {

        // case res.addr just joined
        this.sAddr = res.addr;

      }

    } catch (err) {
    
      console.log("getPredecessor", err);

    }

    try {

      // update successor's predecessor with self
      res = await call(this.sAddr, 'notifySuccessor', { addr: this.addr });

    } catch (err) {
    
      console.log("notifySuccessor", err);

    }

    return res;

  }

  /**
   *
   */
  async get (key) {

    var hash = toSha1(key);

    var res;

    try {

      res = await call(this.addr, 'findSuccessor', { hash });

    } catch (err) {
    
      console.log("findSuccessor", err);

    }

    try {

      res = await call(res.addr, 'getKey', { hash });

    } catch (err) {
    
      console.log("getKey", err);

    }

    return res;

  }

  /**
   *
   */
  async set (key, val) {

    var hash = toSha1(key);

    var val = Buffer.from(val);

    var res;

    try {

      res = await call(this.addr, 'findSuccessor', { hash });

    } catch (err) {
    
      console.log("findSuccessor", err);

    }

    try {

      res = await call(res.addr, 'setKey', { hash, val });

    } catch (err) {
    
      console.log("setKey", err);

    }

    return res;

  }

  /**
   *
   */
  shutdown () {

    return new Promise((resolve, reject) => {

      this.server.tryShutdown(() => {

        // TODO check err and reject, do not clearInterval, etc.

        clearInterval(this.timeout);

        // TODO unbind all events

        resolve({ addr: this.addr, id: this.id });

      });  

    });

  }

  /**
   *
   */
  toString () {

    var str = `PRED ${this.pAddr} (${this.pAddr != null ? toSha1(this.pAddr).toString('hex') : undefined})\n` +
              `SELF ${this.addr} (${this.id})\n` +
              `SUCC ${this.sAddr} (${toSha1(this.sAddr).toString('hex')})\n`;

    for (var hashStr in this.storage) {
      str += `DATA ${hashStr}: ${this.storage[hashStr].toString()}\n`;
    }

    return str;

  }

}

module.exports = {
  Peer
}
