const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const bignum = require('bignum');
const EventEmitter = require('events').EventEmitter;
const Buffer = require('buffer').Buffer;
const chordRPC = grpc.load(__dirname + '/chord.proto').chordRPC;

const M = 3;
const MAX_ID = bignum.pow(2, M);
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
function toSha1 (val) {
  
  return crypto.createHash('sha1').update(val).digest();

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

    var res = await findSuccessor.call(this, id);

    console.log('return', res, '\n');

    this.finger[this.iFinger] = res.addr;

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
    let predecessor = await rpc(this.successorList[0], 'getPredecessor');

    // check alive
    let successorList = await rpc(this.successorList[0], 'getSuccessorList');

    // remove last entry
    successorList.addrs.splice(R - 1, 1);

    // prepend new successor
    successorList.addrs.unshift(this.successorList[0]);

    // reconcile
    this.successorList = successorList.addrs;

    let predecessorIsNotDead = this.predecessor != '';

    if (predecessorIsNotDead && between(toSha1(predecessor.addr), this.id, toSha1(this.successorList[0]))) {
      
      // monitoring technique (do not add invalid successor)
      try {
        
        // check alive
        let successorList = await rpc(predecessor.addr, 'getSuccessorList');

        // remove last entry
        successorList.addrs.splice(R - 1, 1);

        // prepend new successor
        successorList.addrs.unshift(predecessor.addr);

        // reconcile
        this.successorList = successorList.addrs;
      
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
async function findSuccessor (id) {

  // iterate known successors
  for (let successor of this.successorList) {

    // found successor
    if (between(id, this.id, toSha1(successor), false, true)) {

      try {

        // check alive
        await rpc(successor, 'ping');

        // break from loop
        return { addr: successor };

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

        // check alive and forward
        successor = await rpc(successor, 'findSuccessor', { id });

        // break form loop
        return successor;

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

/**
 *
 */
function onGetPredecessor (call, cb) {

  var res = {};

  if (this.predecessor != null) {
    res.addr = this.predecessor;
  }

  cb(null, res);

}

/**
 *
 */
function onGetSuccessorList (call, cb) {

  cb(null, { addrs: this.successorList });

}

/**
 *
 */
async function onFindSuccessor (call, cb) {

  var id = call.request.id;

  try {

    let res = await findSuccessor.call(this, id);

    cb(null, res);

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
function onGet (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');

  if (this.bucket.hasOwnProperty(idStr)) {

    cb(null, { val: this.bucket[idStr]}); 

  } else {

    cb(new Error('invalid key'));

  }

}

/**
 *
 */
function onSet (call, cb) {

  var id = call.request.id;
  
  var idStr = id.toString('hex');
  
  var val = call.request.val;

  // TODO handle some error here and cb(new Error())

  this.bucket[idStr] = val;

  cb(null); 

}

/**
 *
 */
function onGetAll (call, cb) {}

/**
 *
 */
function onSetAll (call, cb) {}

/**
 *
 */
class Peer extends EventEmitter {

  /**
   *
   */
  constructor (port) {

    super();

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
      getPredecessor: onGetPredecessor.bind(this),
      getSuccessorList: onGetSuccessorList.bind(this),
      findSuccessor: onFindSuccessor.bind(this),
      echo: onEcho.bind(this),
      ping: onPing.bind(this),
      notify: onNotify.bind(this),
      get: onGet.bind(this),
      set: onSet.bind(this),
      getAll : onGetAll.bind(this),
      setAll: onSetAll.bind(this)
    });

    // periodic 
    this.timeout = setInterval(() => {
        checkPredecessor.call(this);
        //fixFingers.call(this);
        stabilize.call(this);
    }, 1000);

    // local storage
    this.bucket = {};

    //this.finger = new Array(M);

    //this.finger.fill(undefined);

    //this.iFinger = 0;


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

    // bootstrap steps (do not wait for stabilize)
    try {

      let successor = await rpc(addr, 'findSuccessor', { id: this.id });

      let predecessor = await rpc(successor.addr, 'getPredecessor');

      let successorList = await rpc(predecessor.addr, 'getSuccessorList');

      this.successorList = successorList.addrs;

      this.predecessor = predecessor.addr;


    } catch (err) {
    
      throw new Error('Join Failure' + err);

    }

  }

  /**
   *
   */
  async get (key) {

    var id = toSha1(key);

    var res;

    try {

      res = await findSuccessor.call(this, id);

    } catch (err) {
    
      console.log("get::findSuccessor", err);

    }

    try {

      res.val = (await rpc(res.addr, 'get', { id })).val;

    } catch (err) {
    
      console.log("get::get", err);

    }

    return res;

  }

  /**
   *
   */
  async set (key, val) {

    var id = toSha1(key);

    var val = Buffer.from(val);

    var res;

    try {

      res = await findSuccessor.call(this, id);

    } catch (err) {
    
      console.log("set::findSuccessor", err);

    }

    try {

      await rpc(res.addr, 'set', { id, val });

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

    var str = '';

    str += `<Predecessor> ${this.predecessor ? this.predecessor : '(undefined)'}\n`;

    str += `<Self> ${this.addr}\n`;

    for (let successor of this.successorList) str += `<Successors> ${successor}\n`;

    str += '<Bucket>\n';
    for (let idStr in this.bucket) {
      str += `${idStr} : ${this.bucket[idStr].toString()}\n`;
    }

    // str += '<Fingers>\n';
    // for (let i = 0; i < this.finger.length; i++) {
    //   str += `${(i === this.iFinger) ? '_' : i}: ${(this.finger[i]) ? this.finger[i] : '(undefined)'}\n`;
    // }

    return str;

  }

}

module.exports = {
  Peer
}
