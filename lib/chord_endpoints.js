const Buffer = require("buffer").Buffer;
const utils = require("./utils.js");
const grpc = require("grpc");

const isAddress = utils.isAddress;
const isBetween = utils.isBetween;
const toSHA1 = utils.toSHA1;

const CHORD_PROTO = grpc.load(`${__dirname}/chord.proto`).CHORD_PROTO;
const doRPC = utils.doRPC(CHORD_PROTO);

/**
 * @callback pingCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to ping type RPC
 * @param {object} call
 * @param {pingCallback} cb
 */
function ping(call, cb) {
  const sender = call.request.sender;

  // bad request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  this.emit("ping", sender);

  return cb(null, call.request);
}

/**
 * @callback stateCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to state type RPC
 * @param {object} call
 * @param {stateCallback} cb
 */
function state(call, cb) {
  const response = {};

  if (call.request.predecessor) {
    response.predecessor = this.predecessor;
  }

  if (call.request.successor) {
    response.successor = this.successor;
  }

  if (call.request.finger) {
    response.finger = this.finger;
  }

  return cb(null, response);
}

/**
 * @callback lookupCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to a lookup type RPC
 * @param {object} call
 * @param {lookupCallback} cb
 */
async function lookup(call, cb) {
  /* see Stoica et al. 2001 findSuccessor */

  const keyId = call.request.id;

  // invalid request
  if (!Buffer.isBuffer(keyId)) {
    return cb(new Error());
  }

  const counter = call.request.counter + 1;
  const successor = this.successor[0];
  const successorId = toSHA1(successor);

  // I. Base Case
  // key is between this node and its successor,
  // or key is equal to successor,
  // or this node is its own successor. (this node is singleton)
  if (
    isBetween(this.id, keyId, successorId) ||
    keyId.compare(successorId) === 0
  ) {
    try {
      await doRPC(successor, "ping", { sender: this.address });

      return cb(null, { successor, hops: counter });
    } catch (__) {
      // TODO or ignore
    }

    // II. Recursive Case, Forward Route
  } else {
    for (const hop of this.getPredecessorOf(keyId)) {
      try {
        const response = await doRPC(hop, "lookup", { id: keyId, counter });
        return cb(null, response);
      } catch (__) {
        // immediate successor is dead
        // try again when stabilized
        continue;
      }
    }
  }

  //throw new Error(`invalid successor state ${successor} on ${this.address} hop ${counter}`);
}

/**
 * @callback notifyCallback
 * @param {Error}
 * @param {object}
 */

/**
 * Respond to a notify RPC
 * @param {object} call
 * @param {notifyCallback} cb
 */
async function notify(call, cb) {
  /* see Stoica et. al 2001 notify and Zave 2010 rectify */

  const oldPredecessor = this.predecessor;

  // notifier's address
  const sender = call.request.sender;

  // invalid request
  if (!isAddress(sender)) {
    return cb(new Error());
  }

  // no predecessor,
  if (
    !isAddress(this.predecessor) ||
    // or sender is between predecessor and this node,
    // or predecessor is this node. (this node is transitioning from a singleton)
    isBetween(toSHA1(this.predecessor), toSHA1(sender), this.id)
  ) {
    // update current predecessor
    this.predecessor = sender;
  } else {
    /* current predecessor is defined */

    try {
      await doRPC(this.predecessor, "ping", {
        sender: this.address
      });

      /* current predecessor is alive and valid */
    } catch (e) {
      /* current predecessor is dead */

      // console.error('onNotifyRequest', e);

      // TODO if error is Connect Failed or Timeout
      this.predecessor = sender;
      // else cb(new Error());
    }
  }

  if (this.predecessor !== oldPredecessor) {
    // check if nothing to emit and do not emit for own address
    if (isAddress(oldPredecessor) && oldPredecessor !== this.address) {
      this.emit("predecessor::down", oldPredecessor);
    }
    // do not emit up for own address
    if (this.predecessor !== this.address) {
      this.emit("predecessor::up", this.predecessor);
    }
  }

  this.emit("notify", sender);

  return cb(null);
}

module.exports = {
  ping,
  state,
  lookup,
  notify
};
