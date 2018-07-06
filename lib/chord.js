const _ = require("underscore");
const Buffer = require("buffer").Buffer;
const getPort = require("get-port");
const bignum = require("bignum");
const EventEmitter = require("events").EventEmitter;
const endpoints = require("./chord_endpoints.js");
const utils = require("../lib/utils.js");
const grpc = require("grpc");

const isPort = utils.isPort;
const isIPv4 = utils.isIPv4;
const isAddress = utils.isAddress;
const isBetween = utils.isBetween;
const toSHA1 = utils.toSHA1;

const CHORD_PROTO = grpc.load(`${__dirname}/chord.proto`).CHORD_PROTO;
const doRPC = utils.doRPC(CHORD_PROTO);
const MIN_SUCCESSOR_LENGTH = 1;
const FINGER_LENGTH = 160;
const FINGER_BASE = Array(FINGER_LENGTH)
  .fill(0)
  .map((__, i) => bignum.pow(2, i));
const MAX_ID = bignum.pow(2, 160);

/**
 * Maintain the finger table entries.
 */
const fixFinger = (() => {
  // finger index closure
  let i = 0;

  // do not use arrow function so that context can be bound
  return async function fixFingerClosure() {
    // check index bounds
    if (FINGER_LENGTH <= i) {
      i = 0;
    }

    try {
      // generate finger id
      const id = bignum
        .fromBuffer(this.id)
        .add(FINGER_BASE[i])
        .mod(MAX_ID)
        .toBuffer();

      // find successor
      const response = await doRPC(this.address, "lookup", { id });

      const successor = response.successor;

      // update finger table entry
      this.reverseFinger[FINGER_LENGTH - i - 1] = successor;

      // increment finger index only on success
      i += 1;
    } catch (__) {
      // lookup failed
      // try again next tick when stabilized
      // console.error(__);
      // TODO if error === Connect Failed
      this.reverseFinger[FINGER_LENGTH - i - 1] = undefined;
      // else throw error
    }
  };
})();

/**
 * Maintain the successor list entries. (A.K.A. stabilize)
 */
const fixSuccessor = async function fixSuccessor() {
  let done = false;

  // successor index
  let i = 0;

  // NOTEs
  // - see Zave 2010 StabilizeFromSuccessor step
  do {
    try {
      // grab next immediate successor
      const successor0 = this.successor[i];

      // get predecessor, which is potentially the new successor, and successor list
      let stateResponse = await doRPC(successor0, "state", {
        predecessor: true,
        successor: true
      });

      // pre-adopt the successor list
      let newSuccessor = stateResponse.successor;
      newSuccessor = newSuccessor.slice(0, this.r - 1);
      newSuccessor.unshift(successor0);

      // potential successor address (A.K.A. idealize)
      const potentialSuccessor0 = stateResponse.predecessor;
      const successor0Id = toSHA1(successor0);

      // NOTEs
      // - see Zave 2010 StabilizeFromPredecessor step
      // - true if potentialSuccessor0 just joined
      if (
        isAddress(potentialSuccessor0) &&
        // potential successor is between this node and its successor,
        // or this node is its own successor. (this node is transitioning from a singleton)
        isBetween(this.id, toSHA1(potentialSuccessor0), successor0Id)
      ) {
        try {
          // re-fetch because immediate successor has changed
          stateResponse = await doRPC(potentialSuccessor0, "state", {
            successor: true
          });

          // pre-adopt the successor list
          newSuccessor = stateResponse.successor;
          newSuccessor = newSuccessor.slice(0, this.r - 1);
          newSuccessor.unshift(potentialSuccessor0);
        } catch (__) {
          // potentialSuccessor0 has died while joining so continue with successor0
        }
      }

      // record successors being removed
      const down = _
        .chain(this.successor)
        .difference(newSuccessor)
        .without(this.address)
        .value();

      // record successors being added
      const up = _
        .chain(newSuccessor)
        .difference(this.successor)
        .without(this.address)
        .value();

      // fully adopt the successor list
      this.successor = newSuccessor;

      // successor list has been refreshed and the immediate successor is alive
      done = true;

      // bubble downed-successors to higher-layer
      if (!_.isEmpty(down)) {
        this.emit("successor::down", down);
      }

      // bubble upped-successors to higher-layer
      if (!_.isEmpty(up)) {
        this.emit("successor::up", up);
      }
    } catch (__) {
      // immediate successor had died move to next in list
      i += 1;
    }
  } while (!done && i < this.successor.length);

  // NOTEs
  // - case when successor list is exhausted and stabilization failed
  // - occurs when r = 1 and must repair an involuntary node failure
  // ... or (with low probability) r > 1 and all successors fail involuntary
  // - the next stabilize call will then repair it using StabilizeFromPredecessor step
  // - this has an effect where the immediate successor points back to the predecessor temporarily
  // ... so be aware of this for partition replication (successor up/down events)
  // TODOs
  // - may want to use a timeout and allow the failed node to come back online
  if (!done) {
    /* assert(i === this.successor.length) */

    // bubbled downed successors to higher-layer
    this.emit("successor::down", this.successor);

    // reset successor list
    this.successor.fill(this.address);
  }

  try {
    // notify immediate successor (refresh its predecessor)
    await doRPC(this.successor[0], "notify", {
      sender: this.address
    });
  } catch (__) {
    // ignore failure
  }
};

/**
 * API for starting and stopping the maintenance protocol.
 */
const maintenance = (() => {
  // timeout closure
  let timeout;

  // return API
  return {
    // do not use arrow function so that context can be bound to caller
    start() {
      timeout = setInterval(() => {
        fixFinger.call(this);
        fixSuccessor.call(this);
      }, this._maintenanceInterval);
    },

    stop() {
      clearInterval(timeout);
      timeout = undefined;
    }
  };
})();

/**
 * Class representing a Chord Peer, that provides peer discovery and maintenance protocols.
 * @extends EventEmitter
 */
class Peer extends EventEmitter {
  /**
   * Create an instance of Peer.
   * @param {number} port - The port to bind the peer to.
   * @param {object} options - The optional arguments.
   */
  constructor(options = {}) {
    super();

    _.defaults(options, {
      nSuccessors: MIN_SUCCESSOR_LENGTH
    });

    this._maintenanceInterval = 1000;

    // predecessor address
    this.predecessor = undefined;

    // initialize successor list
    this.r = options.nSuccessors;
    this.successor = new Array(this.r);

    // finger table is maintained in reverse order for easy for..of reverse iteration
    this.successor.fill(undefined);
    this.reverseFinger = new Array(FINGER_LENGTH);
    this.reverseFinger.fill(undefined);

    // instance gRPC server and register chord protocol
    this.server = new grpc.Server();
    this.server.addService(
      CHORD_PROTO.service,
      _.mapObject(endpoints, endpoint => endpoint.bind(this))
    );
  }

  /**
   * Start RPC server with all registered services, and start the maintenance protocol.
   * @param {number} port -
   * @param {string} host -
   * @return {string}
   */
  async listen(port = 0, host = "127.0.0.1") {
    if (port !== 0 && !isPort(port)) {
      throw new Error('"port" argument must be number between 0 and 65536');
    }

    if (!isIPv4(host)) {
      throw new Error('"host" argument must be IPv4-address');
    }

    let portn;
    try {
      portn = await getPort(port);
    } catch (e) {
      throw e;
    }

    this.address = `${host}:${portn}`;
    this.successor.fill(this.address);
    this.id = toSHA1(this.address);
    this.server.bind(this.address, grpc.ServerCredentials.createInsecure());
    this.server.start();
    maintenance.start.call(this);
    return this.address;
  }

  /**
   * Stop RPC server with all registered services, and stop the maintenance protocol.
   */
  close() {
    maintenance.stop();
    this.server.forceShutdown();
    this.id = undefined;
    this.reverseFinger.fill(undefined);
    this.successor.fill(undefined);
    this.address = undefined;
  }

  /**
   * Test if an object is an instance of Peer.
   * @param {object} object -
   * @return {boolean}
   */
  static isPeer(object = {}) {
    return object instanceof Peer;
  }

  /**
   * Test if a peer is joined to a Chord overlay network.
   * @param {Peer} peer -
   * @return {boolean}
   */
  static isJoined(peer = {}) {
    if (!Peer.isPeer(peer)) {
      throw TypeError('"peer" argument must be Peer instance');
    }

    if (!isAddress(peer.address)) {
      throw new Error();
    }

    if (!isAddress(peer.successor[0])) {
      throw new Error();
    }

    return peer.address !== peer.successor[0];
  }

  /**
   * Ping a remote peer.
   * @param {string} host -
   * @return {string}
   */
  async ping(host = this.address) {
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    const t0 = process.hrtime();

    try {
      await doRPC(host, "ping", {
        sender: this.address
      });

      return process.hrtime(t0);
    } catch (e) {
      // bubble error
      throw e;
    }
  }

  /**
   * State info of remote peer.
   * @param {string} host -
   * @return {string}
   */
  async state(host = this.address, withFingers = false) {
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    if (host === this.address) {
      const response = {
        predecessor: this.predecessor,
        address: host,
        successor: this.successor
      };
      if (withFingers) {
        response.finger = this.reverseFinger;
      }
      return response;
    }

    try {
      const response = await doRPC(host, "state", {
        predecessor: true,
        successor: true,
        finger: withFingers
      });
      response.address = host;
      return response;
    } catch (e) {
      throw e;
    }
  }

  /**
   * Join the Chord overlay network that host is participating in.
   * @param {string} host -
   * @return {Promise}
   */
  async join(host) {
    if (Peer.isJoined(this)) {
      throw new Error("cannot join again");
    }

    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    if (host === this.address) {
      throw new Error("cannot join to self");
    }

    try {
      // stop maintenance while updating state
      maintenance.stop();

      // find successor
      const lookupResponse = await doRPC(host, "lookup", {
        id: toSHA1(this.address)
      });

      // immediate successor address
      const newSuccessor0 = lookupResponse.successor;

      // invalid successor response
      if (!isAddress(newSuccessor0)) {
        throw new Error();
      }

      // get predecessor and successor list
      const stateResponse = await doRPC(newSuccessor0, "state", {
        predecessor: true,
        successor: true
      });

      // predecessor address
      const newPredecessor = stateResponse.predecessor;

      // invalid predecessor response
      if (!isAddress(newPredecessor)) {
        throw new Error();
      }

      // adopt predecessor
      this.predecessor = newPredecessor;
      // nothing to emit for down
      this.emit("predecessor::up", this.predecessor);

      // adopt successor list
      let newSuccessor = stateResponse.successor;
      newSuccessor = newSuccessor.slice(0, this.r - 1);
      newSuccessor.unshift(newSuccessor0);
      // do not emit up for own addresses
      const up = _
        .chain(newSuccessor)
        .difference(this.successor)
        .without(this.address)
        .value();
      this.successor = newSuccessor;

      if (!_.isEmpty(up)) {
        this.emit("successor::up", up);
      }

      // NOTEs
      // - "fully joined" only after the first notify event occurs
      // because fixSuccessor on predecessor has only then incorporated this node in ring.
      // - otherwise, bug occurs: partition before set and set before notify
      // - effect of bug: this node joins then sets its own id (or other owned key),
      // set is then done on successor and not this!
      // - okay for set/get/del(lookup) to be called before stabilization, but not partition!
      // - bucket class calls partition on join
      this.once("notify", () => {
        this.emit("join", this.successor[0]);
      });

      return this.successor[0];
    } catch (e) {
      // bubble error
      throw e;
    } finally {
      // ensure maintenance resumes even on failure
      maintenance.start.call(this);
    }
  }

  /**
   * Search the Chord overlay network for the successor of key.
   * @param {Buffer} id -
   * @return {Promise}
   */
  async lookup(id) {
    // invalid request
    if (!Buffer.isBuffer(id)) {
      throw new Error('"id" argument must be string or buffer');
    }

    try {
      // find successor
      const response = await doRPC(this.address, "lookup", { id });

      return response.successor;
    } catch (e) {
      // bubble error
      throw e;
    }
  }

  /**
   * Generator that yields the next successor
   * @return {string}
   */
  *nextSuccessor() {
    for (const successor of this.successor) {
      if (!isAddress(successor)) {
        continue;
      } else {
        yield successor;
      }
    }
    return this.address;
  }

  /**
   * Generator that yields the next live and closest known predecessor of id
   * @param {Buffer} id -
   * @return {string}
   */
  *getPredecessorOf(id) {
    /* see Stoica et al. 2001 and Nowell et al. 2002 closestPrecedingNode */

    if (!Buffer.isBuffer(id)) {
      throw new Error();
    }

    for (const predecessor of this.reverseFinger) {
      if (!isAddress(predecessor)) {
        continue;
      } else if (isBetween(this.id, toSHA1(predecessor), id)) {
        yield predecessor;
      }
    }

    yield* this.nextSuccessor();
  }
}

module.exports = {
  Peer
};
