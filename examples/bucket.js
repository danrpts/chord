'use strict'

const _ = require('underscore');
const endpoints = require('./bucket_endpoints.js');
const Peer = require('../lib/chord.js').Peer
const utils = require('../lib/utils.js');
const toSHA1 = utils.toSHA1
const grpc = require('grpc');
const BUCKET_PROTO = grpc.load(__dirname + '/bucket.proto').BUCKET_PROTO;
const doRPC = utils.doRPC(BUCKET_PROTO);

class Bucket extends Peer {

  constructor (p, n) {

    super(p, n);

    this.hashtable = {}

    this.server.addService(BUCKET_PROTO.service, _.mapObject(endpoints, endpoint => {
      return endpoint.bind(this);
    }));

    // replicate this set to successor
    this.on('successor::up', up => {

      console.log(`\nUP ${up}`);

      try {
        
        // _.each(up, async successor => {
        //   await bucket.transfer(bucket, successor);
        // });

      } catch (e) {
        
        console.log(e);
      
      }

    });

    this.on('successor::down', down => {

      console.log(`\nDOWN ${down}`);

      try {
      
        // _.each(down, async successor => {
        //   await bucket.replicate(bucket, successor);
        // });
      
      } catch (e) {

        console.log(e);
      
      }

    });

    this.on('set', key => {

      try {

        // _.chain(this.successor).without(this.address).each(async successor => {
        //   let mergeResponse = await bucket.merge(bucket, successor);
        // });

      } catch (e) {
        
        console.log(e);
      
      }

    });

  }

  async get (key) {

    const id = toSHA1(key);

    try {

      let location = await this.lookup(key);

      let response = await doRPC(location, 'get', { id });

      return response;

    } catch (e) {

      console.error(e);

    }

  }

  async set (key, value) {

    const id = toSHA1(key);

    value = Buffer.from(value, 'utf8');

    try {

      let location = await this.lookup(key);

      let response = await doRPC(location, 'set', { id, value });

    } catch (e) {

      console.error(e);

    }

  }

  async del (key) {

    const id = toSHA1(key);

    try {

      let location = await this.lookup(key);

      let response = await doRPC(location, 'del', { id });

    } catch (e) {

      console.error(e);

    }

  }

}

module.exports = { Bucket }
