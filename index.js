'use strict';
const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const dht = require('./dht.js');
const arg0 = 'chord';
const argv = minimist(process.argv.slice(2));


function printUsage () {

  var spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [-p <port>]`);
  console.error(`       ${spacer} [-k <replicas>] [--join=<address>]\n`);

}

function printInfo (peer) {

  var info = '';
  info += `<Predecessor> ${(peer.predecessor) ? peer.predecessor : ''}\n`;
  info += `<Self> ${peer.addr}\n`;
  
  var successorList = peer.successorList;
  for (let i in peer.successorList) {
    info += `<Successor ${i}> ${successorList[i]}\n`;
  }

  for (let idStr in peer.bucket) {
    info += `<Bucket ${idStr}> ${peer.bucket[idStr].toString('utf8')}\n`;
  }

  var finger = peer.finger
  for (let i in finger) {
    info += `<Finger ${i}> ${finger[i]}\n`;
  }

  console.log(info);

}

function printHelp () {

  console.error('Commands:');
  console.error(' join <address>            Add peer to network.');
  console.error(' leave                     Remove peer from network.');
  console.error(' get <key>                 Read key from network.');
  console.error(' set <key> [value]         Create/update key in network.');
  console.error(' delete <key>              Delete key and value.');
  console.error(' ping <address>            Ping remote peer.');
  console.error(' echo <address> [message]  Print to remote peer.');
  console.error(' info                      Print info about peer.');
  console.error(' help                      Show this screen.');
  console.error(' quit                      Shutdown peer and exit.\n');

}

if (argv.version) {

  console.log(require('./package.json').version);
  
  process.exit(0);

} else if (argv.help) {

  printUsage();

  printHelp();

  process.exit(0);

}

if (argv._.length > 0 // check for invalid arguments
||  _.has(argv, 'p') && !dht.isPort(argv.p) // invalid port number
||  _.has(argv, 'k') && !_.isNumber(argv.k) // invalid successor/replica count
||  _.has(argv, 'join') && !dht.isAddress(argv.join)) { // invalid address

  // TODO
  // - check invalid options

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  // TODO
  // - peer uses random port when 0

  const peer = dht.createPeer(argv.p, argv.k);

  peer.on('echo', getEcho => {

    console.log(`\n<Echo ${getEcho.addr}> ${getEcho.msg}`);

    rl.prompt();

  });

  // replicate this set to successor
  peer.on('successor', successor => {

    // replicate to new successor

    //peer.replicateTo(successor);


  });

  peer.on('set', (key, value) => {

    // for (let addr of peer.successorList) {
    //   if (addr != peer.addr) console.log(addr);
    // }

    //peer.replicateTo(this.)

  });

  const rl = readline.createInterface({

    input: process.stdin,

    output: process.stdout,

    prompt: "> "

  });

  if (argv.join) {

    peer.join(argv.join);

  }

  rl.prompt();

  rl.on('line', (line) => {

    if (!line) {
      
      rl.prompt();
      
      return;

    }

    let [command, ...args] = line.trim().split(' ');

    args = minimist(args);

    args._ = _.compact(args._);

    // TODO
    // handle empty line

    switch (command) {

      case 'echo':

        var address = args._[0];

        if (!dht.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        var message = args._.slice(1).join(' ');

        peer.echo(address, message);

        rl.prompt();

        break;

      case 'ping':

        var address = args._[0];

        if (!dht.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          var t = process.hrtime();

          var pingResponse = await peer.ping(address);

          t = process.hrtime(t);

          console.log(`<Ping ${address}> ${(t[1] / 1e6).toPrecision(3)} ms`);          

          rl.prompt();

        })();

        break;

      case 'join':

        // TODO
        // - check !isJoined()
        // - maybe use m,r,k params as network id hash
        
        var address = args._[0];

        if (!dht.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        peer.join(address);

        rl.prompt();
        
        break;

      case 'leave':

        // TODO
        // - check isJoined() 

        peer.leave();

        rl.prompt();

        break;

      case 'get':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          var getResponse = await peer.get(key);

          var value = getResponse.value.toString('utf8');

          console.log(`<Bucket ${key}> ${value}`);
          
          rl.prompt();

        })();

        break;

      case 'set':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }

        var value = Buffer.from(args._.slice(1).join(' '));

        peer.set(key, value);
      
        rl.prompt();
        
        break;

      case 'delete':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }
          
        peer.delete(key);
          
        rl.prompt();

        break;

      case 'info':
        
        printInfo(peer);

        rl.prompt();
        
        break;

      case 'quit':

        peer.close();

        process.exit(0);

        break;

      case 'help':

        printHelp();

        rl.prompt();

        break;

      default: 

        printHelp();

        rl.prompt();

        break;
    
    }

  });

  rl.on('close', () => {
    
    process.exit(0);

  });

}
