const _ = require("underscore");
const Buffer = require("buffer").Buffer;
const endpoints = require("./bucket_endpoints.js");
const Peer = require("./chord.js").Peer;
const utils = require("./utils.js");
const grpc = require("grpc");

const isAddress = utils.isAddress;
const toSHA1 = utils.toSHA1;

const BUCKET_PROTO = grpc.load(`${__dirname}/bucket.proto`).BUCKET_PROTO;
const doRPC = utils.doRPC(BUCKET_PROTO);

/**
 * Class representing a Chord Bucket, that provides the ability to store and fetch data in the network.
 * @extends Peer
 */
class Bucket extends Peer {
  /**
   * Create an instance of Bucket.
   * @param {object} options - The optional Peer arguments.
   */
  constructor(options) {
    super(options);

    this.hashtable = {};

    this.server.addService(
      BUCKET_PROTO.service,
      _.mapObject(endpoints, endpoint => endpoint.bind(this))
    );

    // NOTEs
    // - see lib/chord.js join emit for notes on "fully joined"
    this.once("join", this.partition);
  }

  /**
   *
   * @param {string} key -
   * @return {Promise}
   */
  async get(key) {
    const id = toSHA1(key);

    try {
      const host = await this.lookup(id);

      const response = await doRPC(host, "get", { id });

      return response.value;
    } catch (e) {
      throw e;
    }
  }

  /**
   *
   * @param {string} key -
   * @return {Promise}
   */
  async has(key) {
    try {
      await this.get(key);

      return true;
    } catch (e) {
      return false;
    }
  }

  /**
   *
   * @param {string} key -
   * @return {Promise}
   */
  async set(key, value) {
    const id = toSHA1(key);

    try {
      const host = await this.lookup(id);

      await doRPC(host, "set", {
        id,
        value: Buffer.from(value, "utf8")
      });

      return;
    } catch (e) {
      throw e;
    }
  }

  /**
   *
   * @param {string} key -
   * @return {Promise}
   */
  async del(key) {
    const id = toSHA1(key);

    try {
      const host = await this.lookup(id);

      await doRPC(host, "del", { id });

      return;
    } catch (e) {
      throw e;
    }
  }

  /**
   *
   * @param {string} host -
   * @return {Promise}
   */
  async dump(host = this.address) {
    if (!isAddress(host)) {
      return new Error('"host" argument must be compact IP-address:port');
    }

    if (host === this.address) {
      return this.hashtable;
    }

    try {
      const response = await doRPC(host, "partition", {
        id: toSHA1(host),
        delete: false
      });
      const entries = response.entries;
      const table = {};

      if (!_.isArray(entries)) {
        throw new Error();
      }

      entries.forEach(entry => {
        table[entry.id.toString("hex")] = entry.value;
      });

      return table;
    } catch (e) {
      throw e;
    }
  }

  /**
   * Get and set partition from host.
   * @param {string} host -
   * @return {Promise}
   */
  async partition(host) {
    if (!isAddress(host)) {
      throw new Error();
    }

    try {
      const response = await doRPC(host, "partition", {
        id: this.id,
        delete: true
      });
      const entries = response.entries;

      if (!_.isArray(entries)) {
        throw new Error();
      }

      entries.forEach(entry => {
        this.hashtable[entry.id.toString("hex")] = entry.value;
      });

      return entries;
    } catch (e) {
      throw e;
    }
  }

  /**
   *
   * @return {Array}
   */
  toArray() {
    const entries = [];
    Object.keys(this.hashtable).forEach(key => {
      const keyId = Buffer.from(key, "hex");
      entries.push({
        id: keyId,
        value: this.hashtable[key]
      });
    });
  }
}

module.exports = {
  Bucket
};
