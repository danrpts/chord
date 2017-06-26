const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const Buffer = require('buffer').Buffer;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

const M = 10; // finger table entries
const MAX_ID = bignum.pow(2, 160); // must be 160 because using sha1 even though finger table has 3 entries
const FINGER_BASE = new Array(M).fill(undefined).map((_,i) => bignum.pow(2, i));
const R = 3;

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
function toSha1 (value) {
  
  return crypto.createHash('sha1').update(value).digest();

}

/**
 *
 */
function between (el, lwr, upr, lwrIcl = false, uprIcl = false) {

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
async function checkPredecessor () {

  try {

    if (this.predecessor) await rpc(this.predecessor, 'ping');

  } catch (err) {

    // TODO if err === Connect Failed
    this.predecessor = null;
    // else throw error
    console.log("checkPredecessor::ping", err);

  }
  
}

/**
 *
 */
async function fixFingers () {

  // generate finger id meaning this.id + 2^k
  var id = bignum.fromBuffer(this.id)
                 .add(FINGER_BASE[this.iFinger])
                 .mod(MAX_ID)
                 .toBuffer();
  
  try {

    var findSuccessorResponse = await findSuccessor.call(this, id);

    this.finger[this.iFinger] = findSuccessorResponse.addr;

  } catch (err) {

    console.log("fixFingers::findSuccessor", err);

  } finally {

    this.iFinger += 1;

    if (M <= this.iFinger) {
      this.iFinger = 0;
    }

  }

}

/**
 *
 */
async function rectify (notifier) {

  try {
    
    await rpc(this.predecessor, 'ping');
  
  } catch (err) {
  
    // TODO if err is Connect Failed or Timeout
    
    this.predecessor = null;
    
    // else break finally and throw exception

  } finally {

    let predecessorIsDead = this.predecessor === null;

    if (predecessorIsDead || between(toSha1(notifier.addr), toSha1(this.predecessor), this.id)) {

      this.predecessor = notifier.addr;
    
    }

  }

}

/**
 *
 */
async function notify () {

  // update successor's predecessor
  try {

    await rpc(this.successorList[0], 'notify', { addr: this.addr });

  } catch (_) {/* ignore*/}

}

/**
 *
 */
async function stabilize () {

  // fix successors
  try {

    //console.log(this.successorList);

    // update successor when node joined in between
    let getPredecessorResponse = await rpc(this.successorList[0], 'getPredecessor');

    // check alive
    let successorListResponse = await rpc(this.successorList[0], 'getSuccessorList');

    // remove last entry
    successorListResponse.addrs.splice(R - 1, 1);

    // prepend new successor
    successorListResponse.addrs.unshift(this.successorList[0]);

    // reconcile
    this.successorList = successorListResponse.addrs;

    let predecessorIsNotDead = this.predecessor != '';

    if (predecessorIsNotDead && between(toSha1(getPredecessorResponse.addr), this.id, toSha1(this.successorList[0]))) {
      
      // monitoring technique (do not add invalid successor)
      try {
        
        // check alive
        successorListResponse = await rpc(getPredecessorResponse.addr, 'getSuccessorList');

        // remove last entry
        successorListResponse.addrs.splice(R - 1, 1);

        // prepend new successor
        successorListResponse.addrs.unshift(getPredecessorResponse.addr);

        // reconcile
        this.successorList = successorListResponse.addrs;
      
      } catch (_) {/* ignore*/}


    }

  // immediate successor is unavailable
  } catch (err) {

    // TODO if err === Connect Failed or Timeout {

    // remove dead node
    this.successorList.shift();

    this.successorList.push(this.addr);

    // NOTE next period will stabilize the new successor

    // } else break finally and throw exception

  } finally {

    notify.call(this);

  }

}

/**
 *
 */
function* closestPrecedingNode (id) {

  // iterate finger tab;e
  for (var i = M - 1; i > -1; i--) {

    let finger = this.finger[i];

    // finger table is cold
    if (!isAddress(finger)) continue;

    let fingerId = toSha1(finger);

    // found a good finger
    if (between(fingerId, this.id, id)) {
      
      yield finger;

    }

  }

  // no finger is applicable so default to successor list
  for (let successor of this.successorList) {

    yield successor;

  }

}

/**
 *
 */
async function findSuccessor (id) {

  // simple optimization
  if (this.id.compare(id) === 0) {

    return { addr: this.addr };

  } else {

    for (let predecessorAddr of closestPrecedingNode.call(this, id)) {

      // found successor
      if (between(id, this.id, toSha1(predecessorAddr), false, true)) {

        try {

          // check alive
          await rpc(predecessorAddr, 'ping');

          // break from outer loop
          return { addr: predecessorAddr };

        // successor is dead
        } catch (err) {

          // if err === Connect Failed or Timeout
          // try next successor
          continue;
          // else throw exception

        }

      // forward to first live successor
      } else {

        try {

          //console.log(`findSuccessor calling... ${predecessorAddr}`);

          // check alive and forward
          let findSuccessorResponse = await rpc(predecessorAddr, 'findSuccessor', { id });

          // break from outer loop
          return findSuccessorResponse;

        // successor is dead
        } catch (err) {

          // if err === Connect Failed or Timeout
          // try next successor
          continue;
          // else throw exception

        }

      }

    }

  }

  // default
  //return { addr: this.successorList[0] };

}

/**
 *
 */
function onGetPredecessorRequest (call, cb) {

  var getPredecessorResponse = {};

  if (this.predecessor != null) {
    getPredecessorResponse.addr = this.predecessor;
  }

  cb(null, getPredecessorResponse);

}

/**
 *
 */
function onGetSuccessorListRequest (call, cb) {

  cb(null, { addrs: this.successorList });

}

/**
 *
 */
async function onFindSuccessorRequest (call, cb) {

  try {

    let findSuccessorResponse = await findSuccessor.call(this, call.request.id);

    //console.log(`onFindSuccessor return... ${findSuccessorResponse.addr}`);

    cb(null, findSuccessorResponse);

  } catch (err) {

    cb(err);

  }

}

/**
 *
 */
function onEchoRequest (call, cb) {

  this.emit('echo', call.request);

  cb(null);

}

/**
 *
 */
function onPingRequest (call, cb) {

  cb(null, call.request);

}

/**
 *
 */
function onNotifyRequest (call, cb) {

  try {
    
    rectify.call(this, call.request);
  
    cb(null);

  } catch (err) {

    cb(err);

  }

}

/**
 *
 */
function onGetRequest (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');

  if (this.bucket.hasOwnProperty(idStr)) {

    cb(null, { value: this.bucket[idStr] }); 

  } else {

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onSetRequest (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');
  
  var value = call.request.value.toString('utf8');

  this.bucket[idStr] = value;

  // TODO handle error

  cb(null); 

}

/**
 *
 */
function onGetAllRequest (call, cb) {

  var bucketEntries = [];

  for (let idStr in this.bucket) {

    let id = Buffer.from(idStr, 'hex');

    if (between(id, toSha1(this.predecessor), call.request.id, false, true)) {

      let value = Buffer.from(this.bucket[idStr], 'utf8');

      bucketEntries.push({ id, value });

      delete this.bucket[idStr];

    }

  }

  cb(null, bucketEntries);

}

/**
 *
 */
function onSetAllRequest (call, cb) {

  for (let entry of call.request.bucketEntries) {

    var idStr = entry.id.toString('hex');
  
    var value = entry.value;

    this.bucket[idStr] = value.toString('utf8');

  }
  
  // TODO handle error

  cb(null);

}

/**
 *
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port, options) {

    super();

    // _.defaults(options, {

    //   bucketKeyEncoding: 'hex',

    //   bucketValueEncoding: 'utf8'

    // });

    // set address
    this.addr = ip.address() + ':' + port;

    if (!isAddress(this.addr)) {
      throw new Error('invalid ip');
    }

    // initialize node identifier
    this.id = toSha1(this.addr);
    
    // predecessor address
    this.predecessor = null;

    // successor list
    this.successorList = new Array(R);

    this.successorList.fill('');

    this.successorList[0] = this.addr;

    // start RPC server
    this.server = listen(this.addr, {
  
      getPredecessor: onGetPredecessorRequest.bind(this),
  
      getSuccessorList: onGetSuccessorListRequest.bind(this),
  
      findSuccessor: onFindSuccessorRequest.bind(this),
    
      echo: onEchoRequest.bind(this),
    
      ping: onPingRequest.bind(this),
    
      notify: onNotifyRequest.bind(this),
    
      get: onGetRequest.bind(this),
    
      set: onSetRequest.bind(this),
    
      getAll : onGetAllRequest.bind(this),
    
      setAll: onSetAllRequest.bind(this)
    
    });

    // periodic 
    this.timeout = setInterval(() => {

        checkPredecessor.call(this);
      
        fixFingers.call(this);
      
        stabilize.call(this);
    
    }, 1000);

    // local bucket storage
    this.bucket = {};

    this.finger = new Array(M);

    this.finger.fill('');

    this.iFinger = 0;

  }

  /**
   *
   */
  async echo (addr, msg) {

    var getEchoResponse;

    try {

      getEchoResponse = await rpc(addr, 'echo', { msg, addr: this.addr });

    } catch (err) {

      console.log('echo::echo', err);

    }

    return getEchoResponse;

  }

  /**
   *
   */
  async ping (addr) {

    var getPingResponse;

    function hrtimeObj (rel) {

      var [secs, nans] = process.hrtime(rel);
  
      return { secs, nans };

    }

    try {

      getPingResponse = await rpc(addr, 'ping', hrtimeObj());

      getPingResponse.dif = hrtimeObj([getPingResponse.secs, getPingResponse.nans]);

    } catch (err) {
    
      console.log("ping::ping", err);

    }

    return getPingResponse;

  }

  /**
   *
   */
  async join (addr) {

    // bootstrap steps (do not wait for stabilize)
    try {

      let findSuccessorResponse = await rpc(addr, 'findSuccessor', { id: this.id });

      let getAllResponse = await rpc(findSuccessorResponse.addr, 'getAll', { id: this.id });

      for (let entry of getAllResponse.bucketEntries) {

        this.bucket[entry.id.toString('hex')] = entry.value.toString('utf8');

      }

      let getPredecessorResponse = await rpc(findSuccessorResponse.addr, 'getPredecessor');

      let getSuccessorListResponse = await rpc(getPredecessorResponse.addr, 'getSuccessorList');

      this.successorList = getSuccessorListResponse.addrs;

      this.predecessor = getPredecessorResponse.addr;


    } catch (err) {
    
      console.log('Join Failure', err);

    }

  }

  /**
   *
   */
  async leave () {

    var bucketEntries = [];

    for (let idStr in this.bucket) {

      let id = Buffer.from(idStr, 'hex');
      
      let value = Buffer.from(this.bucket[idStr], 'utf8');
      
      bucketEntries.push({ id, value });
    
    }

    try {

      //let successor = await findSuccessor.call(this, this.id);

      await rpc(this.successorList[0], 'setAll', { bucketEntries });

    } catch (err) {

      console.log('leave::rpc::setAll', err);

    }

  }

  shutdown () {

    this.leave();

    return new Promise((resolve, reject) => {

      this.server.tryShutdown(err => {

        // TODO unbind all events

        if (err) {

          reject(err);

        } else {

          clearInterval(this.timeout);

          resolve();

        }

      });  

    });

  }

  /**
   *
   */
  async get (key) {

    var id = toSha1(key);

    var findSuccessorResponse;

    var getResponse;

    try {

      findSuccessorResponse = await findSuccessor.call(this, id);

    } catch (err) {
    
      console.log("get::findSuccessor", err);

    }

    try {

      getResponse = await rpc(findSuccessorResponse.addr, 'get', { id });

    } catch (err) {
    
      console.log("get::rpc::get", err);

    }

    return getResponse;

  }

  /**
   *
   */
  async set (key, value) {

    var id = toSha1(key);

    var value = Buffer.from(value, 'utf8');

    var findSuccessorResponse;

    try {

      findSuccessorResponse = await findSuccessor.call(this, id);

    } catch (err) {
    
      console.log("set::findSuccessor", err);

    }

    try {

      await rpc(findSuccessorResponse.addr, 'set', { id, value });

    } catch (err) {
    
      console.log("set::rpc::set", err);

    }

  }

  /**
   *
   */
  toString (b) {

    var str = '';

    str += `<Predecessor> ${(this.predecessor) ? this.predecessor : ''}\n`;

    str += `<Self> ${this.addr}\n`;

    for (let i = 0; i < this.successorList.length; i++) {
      str += `<Successor ${i}> ${this.successorList[i]}\n`;
    }

    for (let idStr in this.bucket) str += `<Bucket ${idStr}> ${this.bucket[idStr].toString('utf8')}\n`;

    for (let i = 0; i < this.finger.length; i++) {
      str += `<Finger ${i}> ${this.finger[i]}\n`;
    }

    return str;

  }

}

module.exports = {
  Peer
}
