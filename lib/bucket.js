'use strict'

const _ = require('underscore');
const Buffer = require('buffer').Buffer
const endpoints = require('./bucket_endpoints.js');
const Peer = require('./chord.js').Peer
const utils = require('./utils.js');
const isAddress = utils.isAddress
const toSHA1 = utils.toSHA1
const grpc = require('grpc');
const BUCKET_PROTO = grpc.load(__dirname + '/bucket.proto').BUCKET_PROTO;
const doRPC = utils.doRPC(BUCKET_PROTO);

class Bucket extends Peer {

  constructor (options) {

    super(options);

    this.hashtable = {}

    this.server.addService(BUCKET_PROTO.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));

    // replicate this set to successor
    this.on('successor::up', up => {

      //console.log(`\nUP ${up}`);

      try {
        
        // _.each(up, async successor => {
        //   await bucket.transfer(bucket, successor);
        // });

      } catch (e) {
        
        console.error(e);
      
      }

    });

    this.on('successor::down', down => {

      //console.log(`\nDOWN ${down}`);

      try {
      
        // _.each(down, async successor => {
        //   await bucket.replicate(bucket, successor);
        // });
      
      } catch (e) {

        console.error(e);
      
      }

    });

    this.on('set', key => {

      try {

        // _.chain(this.successor).without(this.address).each(async successor => {
        //   let mergeResponse = await bucket.merge(bucket, successor);
        // });

      } catch (e) {
        
        console.error(e);
      
      }

    });

    this.on('join', async successor0 => {

      try {

        var response = await doRPC(successor0, 'partition', {
          id: this.id
        });

        let partition = response.partition;

        // invalid response
        if (!_.isArray(partition)) {
          return cb(new Error());
        }

        for (let entry of partition) {
          let key = entry.id.toString('hex');
          this.hashtable[key] = entry.value;
        }

      } catch (e) {
        console.error(e);
        process.exit(1);
      } 

    });

  }

  async get (key) {

    const id = toSHA1(key);

    try {

      let address = await this.lookup(key);

      let response = await doRPC(address, 'get', { id });

      return response.value

    } catch (e) {

      throw e

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

      let address = await this.lookup(id);

      await doRPC(address, 'set', { 
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

      let address = await this.lookup(key);

      let response = await doRPC(address, 'del', { id });

      return

    } catch (e) {

      throw e

    }

  }

  async all (hostname) {

    if (!isAddress(hostname)) {
      throw new Error();
    }

    try {

      let response = await doRPC(hostname, 'all', {
        sender: this.address
      });

      let all = {}

      for (let entry of response.all) {
        all[entry.id.toString('hex')] = entry.value
      }

      return all

    } catch (e) {

      throw e

    }

  }

}

module.exports = { Bucket }
