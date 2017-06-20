const ip = require('ip');
const grpc = require('grpc');
const crypto = require('crypto');
const buffer = require('buffer').Buffer;
const PROTO_PATH = __dirname + '/chord.proto';

/**
 *
 */
function connect (addr) {

  const chordRPC = grpc.load(PROTO_PATH).chord;

  return new chordRPC(addr, grpc.credentials.createInsecure());

}

/**
 *
 */
function disconnect (client) {

  grpc.closeClient(client);

}

/**
 *
 */
function listen (addr, api) {


  const chordRPC = grpc.load(PROTO_PATH).chord;

  var server = new grpc.Server();
  
  server.addService(chordRPC.service, api);
  
  server.bind(addr, grpc.ServerCredentials.createInsecure());

  server.start();

  return server;

}

/**
 *
 */
function isAddress (addr) {

  addr = (typeof addr === 'string') ? addr : '';

  var [ip4, port] = addr.trim().split(':');

  port = parseInt(port);

  return !!port && (1 <= port) && (port <= 65535) && ip.isV4Format(ip4);

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
function getTime (relative) {

  var [s, n] = process.hrtime(relative);
  
  return { secs: s, nans: n };


}
/**
 *
 */
function between (el, lwr, upr, uprIncl = false) {

  // lower before upper
  if (lwr.compare(upr) < 0) {

    // lower before hash AND hash before/at upper
    return (lwr.compare(el) < 0 && el.compare(upr) < 0) || (uprIncl && el.compare(upr) === 0);

  // upper before lower
  } else {

    // lower before hash OR hash before/at upper
    return (lwr.compare(el) < 0) || (el.compare(upr) < 0) || (uprIncl && el.compare(upr) === 0);

  }

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
function onGetPredecessor (call, cb) {

  var pAddr = (this.pAddr != null) ? this.pAddr : '\0';

  cb(null, { addr: pAddr });

}

/**
 *
 */
function onGetSuccessor (call, cb) {

  cb(null, { addr: this.sAddr });

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
  if (between(hash, this._idHash, toSha1(this.sAddr), true)) {

    // notify client of the bucket address
    cb(null, { addr: this.sAddr });

  // forward the lookup to the peer
  } else {

    try {

     var s = await this.lookup(this.sAddr, hash);

     cb(null, s);

    } catch (e) {
    
      cb(e);

    }

  }

}

/**
 *
 */
function onNotify (call, cb) {

  var addr = call.request.addr;

  if (this.pAddr === null || between(toSha1(addr), toSha1(this.pAddr), this._idHash)) {

    this.pAddr = addr;

  }

  cb(null, { addr: this.pAddr });

}

class Peer {

  /**
   *
   */
  constructor (port) {

    this.addr = ip.address() + ':' + port;

    if (!isAddress(this.addr)) {
      throw new Error('Invalid IP-address/port');
    }

    this._idHash = toSha1(this.addr);
    this.id = this._idHash.toString('hex');

    this.pAddr = null; // predecessor address
    this.sAddr = this.addr; // successor address

    this.server = listen(this.addr, {
      ping: onPing.bind(this),
      getPredecessor: onGetPredecessor.bind(this),
      getSuccessor: onGetPredecessor.bind(this),
      findSuccessor: onFindSuccessor.bind(this),
      notify: onNotify.bind(this)
    });

    // TODO NOTE
    // in constructor for a reason
    this.timeout = setInterval(this.stabilize.bind(this), 5000);

    this.storage = {};

  }

  /**
   *
   */
  ping (addr) {

    if (!isAddress(addr)) {
      throw new Error('Invalid IP-address/port: ' + addr);
    }

    var client = connect(addr);  

    return new Promise((resolve, reject) => {

      client.ping(getTime(), (err, res) => {

        disconnect(client);

        if (err) reject(err);

        else {
          
          res.dif = getTime([res.secs, res.nans]);
          
          resolve(res);

        }

      });

    });

  }

  /**
   *
   */
  lookup (addr, hash) {

    if (!isAddress(addr)) {
      throw new Error('Invalid IP-address/port: ' + addr);
    }

    if (!buffer.isBuffer(hash)) {
      throw new TypeError('Invalid hash type');
    }

    var client = connect(addr);

    return new Promise((resolve, reject) => {

      client.findSuccessor({ hash }, (err, res) => {
        
        disconnect(client);

        if (err) reject(err);
        
        else resolve(res);

      });

    });

  }

  /**
   *
   */
  async join (addr) {

    if (!isAddress(addr)) {
      throw new Error('Invalid IP-address/port: ' + addr);
    }

    var suc;

    try {

      suc = await this.lookup(addr, this._idHash);

      this.pAddr = null;
      
      this.sAddr = suc.addr;
    
    } catch (e) {
    
      console.log("lookup error", e);

      process.exit(1);

    }

    return suc;

  }

  /**
   *
   */
  async stabilize () {

    try {

      // update successor

      var successor = connect(this.sAddr);

      successor.getPredecessor({ addr: this.addr }, (err, res) => {
        
        var sIdHash = toSha1(this.sAddr);

        if (between(toSha1(res.addr), this._idHash, sIdHash)) {

          // pAddr just joined the network
          this.sAddr = res.addr;

        }

      });

      // update successor's predecessor

      successor.notify({ addr: this.addr }, (err, res) => {
        
        disconnect(successor);

      });


    } catch (e) {
    
      console.log("lookup error", e);

      process.exit(1);

    }

  }

  /**
   *
   */
  shutdown (cb) {
    
    this.server.tryShutdown(cb);

  }

  /**
   *
   */
  toString () {

    var str = `PRED ${this.pAddr} (${this.pAddr != null ? toSha1(this.pAddr).toString('hex') : undefined})\n` +
              `SELF ${this.addr} (${this.id})\n` +
              `SUCC ${this.sAddr} (${toSha1(this.sAddr).toString('hex')})`;

    for (var hash in this.storage) {
      str += `DATA ${hash}: ${this.storage[hash]}\n`;
    }

    return str;

  }

}

module.exports = {
  Peer
}
