'use strict'

const _ = require('underscore');
const Buffer = require('buffer').Buffer;
const endpoints = require('./bucket_endpoints.js');
const Peer = require('./chord.js').Peer
const utils = require('./utils.js');
const isAddress = utils.isAddress;
const toSHA1 = utils.toSHA1;
const grpc = require('grpc');
const BUCKET_PROTO = grpc.load(__dirname + '/bucket.proto').BUCKET_PROTO;
const doRPC = utils.doRPC(BUCKET_PROTO);

class Bucket extends Peer {

  constructor (options) {

    super(options);

    this.hashtable = {};

    this.server.addService(BUCKET_PROTO.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));

    // NOTEs
    // - see lib/chord.js join emit for notes on "fully joined"
    this.once('join', this.partition);

  }

  async get (key) {

    const id = toSHA1(key);

    try {

      let host = await this.lookup(key);

      let response = await doRPC(host, 'get', { id });

      return response.value;

    } catch (e) {

      throw e;

    }

  }

  async has (key) {

    try {

      await this.get(key);

      return true;

    } catch (e) {

      return false;

    }

  }

  async set (key, value) {

    const id = toSHA1(key);

    try {

      let host = await this.lookup(id);

      await doRPC(host, 'set', { 
        id,
        value: Buffer.from(value, 'utf8') 
      });

      return;

    } catch (e) {

      throw e;

    }

  }

  async del (key) {

    const id = toSHA1(key);

    try {

      let host = await this.lookup(key);

      let response = await doRPC(host, 'del', { id });

      return;

    } catch (e) {

      throw e;

    }

  }

  async all (host) {

    if (!isAddress(host)) {
      throw new Error();
    }

    try {

      let response = await doRPC(host, 'all', {
        sender: this.address
      });

      let all = {};

      for (let entry of response.all) {
        all[entry.id.toString('hex')] = entry.value;
      }

      return all;

    } catch (e) {

      throw e;

    }

  }

  /**
   * Get and set partition from host.
   * @param {string} host -
   * @return {Promise}
   */
  async partition (host) {

    if (!isAddress(host)) {
      throw new Error();
    }

    try {

      var response = await doRPC(host, 'partition', { id: this.id });

      let partition = response.partition;

      // invalid response
      if (!_.isArray(partition)) {
        return cb(new Error());
      }

      for (let entry of partition) {
        let key = entry.id.toString('hex');
        this.hashtable[key] = entry.value;
      }

      return partition;

    } catch (e) {
      
      throw e;

    }

  }

}

module.exports = { Bucket }
