const grpc = require('grpc');
const PROTO_PATH = __dirname + '/drpc.proto';
const drpc = grpc.load(PROTO_PATH).drpc;
const helpers = require('./drpc_helpers.js');

function start (address, api) {

}