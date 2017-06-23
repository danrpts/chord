const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const Buffer = require('buffer').Buffer;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

const M = 160;
const MAX_ID = bignum.pow(2, M);
const FINGER_BASE = new Array(M).fill(undefined).map((_,i) => bignum.pow(2, i));

/**
 *
 */
function isAddress (addr) {

  if (typeof addr != 'string') return false;

  var [ip4, port] = addr.trim().split(':');

  port = parseInt(port);

  if (!Number.isSafeInteger(port)) return false;

  return (1024 <= port) && (port <= 65536) && ip.isV4Format(ip4);

}

/**
 *
 */
function rpc (addr, method, req) {

  if (!isAddress(addr)) {
    throw new Error('"addr" argument must be compact IP-address:port');
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
 * closest known preceding node
 */
function closestPrecedingNode (hash) {

  // NOTE maybe i = this.iFinger and remove isAddress
  for (var i = M - 1; i > -1; i--) {

    var peer = this.finger[i];

    // finger table is cold
    if (!isAddress(peer)) continue;

    if (between(toSha1(peer), toSha1(this.addr), hash)) {
      
      return peer;

    }

  }

  // return successor
  return this.finger[0];

}

/**
 *
 */
async function findSuccessorHelper (hash) {


  if (between(hash, this._idHash, toSha1(this.sAddr), false, true)) {

    return { addr: this.sAddr };

  } else {

    var peer = closestPrecedingNode.call(this, hash);

    return rpc(peer, 'findSuccessor', { hash });

  }

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

  try {

    let res = await findSuccessorHelper.call(this, hash);

    cb(null, res.addr);

  } catch (err) {

    cb(err);

  }

}

/**
 *
 */
function onEcho (call, cb) {

  this.emit('echo', call.request);

  cb(null);

}

/**
 *
 */
function onPing (call, cb) {

  cb(null, call.request);

}

/**
 *
 */
function onNotify (call, cb) {

  var addr = call.request.addr;

  if (this.pAddr === null || between(toSha1(addr), toSha1(this.pAddr), this._idHash)) {
    this.pAddr = addr;
  }

  cb(null);

}

/**
 *
 */
function onGet (call, cb) {

  var hops = call.request.hops;

  var hash = call.request.hash;
  
  var hashStr = hash.toString('hex');

  if (this.bucket.hasOwnProperty(hashStr)) {

    cb(null, { val: this.bucket[hashStr]}); 

  } else {

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onSet (call, cb) {

  var hops = call.request.hops;

  var hash = call.request.hash;
  
  var hashStr = hash.toString('hex');
  
  var val = call.request.val;

  // TODO handle some error here and cb(new Error())

  this.bucket[hashStr] = val;

  cb(null); 

}

/**
 *
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port) {

    super();

    this.addr = ip.address() + ':' + port;

    if (!isAddress(this.addr)) {

      throw new Error('"addr" argument must be compact IP-address:port');

    }

    this._idHash = toSha1(this.addr);
    
    this.id = this._idHash.toString('hex');

    this.pAddr = null; // predecessor address
    
    this.sAddr = this.addr; // successor address

    this.server = listen(this.addr, {
    
      getPredecessor: onGetPredecessor.bind(this),
    
      getSuccessor: onGetPredecessor.bind(this),
    
      findSuccessor: onFindSuccessor.bind(this),

      echo: onEcho.bind(this),
    
      ping: onPing.bind(this),

      notify: onNotify.bind(this),
    
      get: onGet.bind(this),

      set: onSet.bind(this)
    
    });

    this.timeout = setInterval(this.stabilize.bind(this), 1000);

    this.bucket = {};

    this.finger = new Array(M);

    this.iFinger = 0;

  }

  /**
   *
   */
  async echo (addr, msg) {

    var res;

    try {

      res = await rpc(addr, 'echo', { msg, addr: this.addr });

    } catch (err) {

      console.log('echo::echo', err);

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

      res = await rpc(addr, 'ping', hrtimeObj());

      res.dif = hrtimeObj([res.secs, res.nans]);

    } catch (err) {
    
      console.log("ping::ping", err);

    }

    return res;

  }

  /**
   *
   */
  async join (addr) {

    var res;  

    try {

      res = await rpc(addr, 'findSuccessor', { hash: this._idHash });

      this.pAddr = null;
      
      this.sAddr = res.addr;

    } catch (err) {
    
      console.log("join::findSuccessor", err);

    }

    return res;

  }

  /**
   *
   */
  async stabilize () {

    // check predecessor
    try {

      if (this.pAddr) await rpc(this.pAddr, 'ping');

    } catch (err) {

      // fix predecessor
      this.pAddr = null;

      console.log("stabilize::ping", err);

    }

    // fix fingers
    try {

      var hash = bignum.fromBuffer(this._idHash).add(FINGER_BASE[this.iFinger]).mod(MAX_ID).toBuffer();

      this.iFinger += 1;
      
      if (M <= this.iFinger) {
        this.iFinger = 0;
      }


      var res = await findSuccessorHelper.call(this, hash);

      this.finger[this.iFinger] = res.addr;

    } catch (err) {

      // TODO if err === Connect Failed

      // fix successor
      this.sAddr = this.addr;

      console.log("stabilize::findSuccessor", err);

    }

    // update successor
    try {

      let res = await rpc(this.sAddr, 'getPredecessor');

      if (res.addr != '' && between(toSha1(res.addr), this._idHash, toSha1(this.sAddr))) {

        // case res.addr just joined
        this.sAddr = res.addr;

      }

    } catch (err) {
    
      // fix successor
      this.sAddr = this.addr;

      console.log("stabilize::getPredecessor", err);

    }

    // update successor's predecessor
    try {

      await rpc(this.sAddr, 'notify', { addr: this.addr });

    } catch (err) {
    
      // fix successor
      this.sAddr = this.addr;

      console.log("stabilize::notify", err);

    }

  }

  /**
   *
   */
  async get (key) {

    var hash = toSha1(key);

    var res;

    try {

      res = await rpc(this.addr, 'findSuccessor', { hash });

    } catch (err) {
    
      console.log("get::findSuccessor", err);

    }

    try {

      res.val = (await rpc(res.addr, 'get', { hash })).val;

    } catch (err) {
    
      console.log("get::get", err);

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

      res = await rpc(this.addr, 'findSuccessor', { hash });

    } catch (err) {
    
      console.log("set::findSuccessor", err);

    }

    try {

      await rpc(res.addr, 'set', { hash, val });

    } catch (err) {
    
      console.log("set::set", err);

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
  toString (b) {

    var str = `PRED ${this.pAddr} (${this.pAddr != null ? toSha1(this.pAddr).toString('hex') : undefined})\n` +
              `SELF ${this.addr} (${this.id})\n` +
              `SUCC ${this.sAddr} (${toSha1(this.sAddr).toString('hex')})`;

    for (var hashStr in this.bucket) {
      str += `\nDATA ${hashStr}: ${this.bucket[hashStr].toString()}`;
    }

    if (b) {
      this.finger.forEach((finger, i) => {
        str += `\nFNGR ${i} : ${finger}`;
      });
    }

    return str;

  }

}

module.exports = {
  Peer
}
