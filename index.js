'use strict';

const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');

const dht = require('./dht.js');

// TODO
// -
const arg0 = 'chord';
const argv = minimist(process.argv.slice(2));
argv._ = _.compact(argv._);


function printUsage () {

  var spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [-p <port>]`);
  console.error(`       ${spacer} [-r <replicas>] [--join=<address>]\n`);

}

function printInfo (peer) {

  var info = '';
  info += `<Predecessor> ${(peer.predecessor) ? peer.predecessor : ''}\n`;
  info += `<Self> ${peer.address}\n`;
  
  var successorList = peer.successorList;
  for (let i in peer.successorList) {
    info += `<Successor ${i}> ${successorList[i]}\n`;
  }

  for (let idStr in peer.bucket) {
    info += `<Entry ${idStr}> ${peer.bucket[idStr].toString('utf8')}\n`;
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
  console.error(' quit                      Leave network and exit.');
  console.error(' info                      Print info about peer.');
  console.error(' help                      Show this screen.\n');

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
||  _.has(argv, 'p') && !dht.Peer.isPort(argv.p) // invalid port number
||  _.has(argv, 'm') && !_.isNumber(argv.m) // invalid finger count
||  _.has(argv, 'r') && !_.isNumber(argv.r) // invalid successor/replica count
||  _.has(argv, 'join') && !dht.Peer.isAddress(argv.join)) { // invalid address

  // TODO
  // - check invalid options

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  // TODO
  // - peer uses random port when 0

  const peer = dht.createPeer(argv.p, argv.m, argv.r);

  peer.on('echo', echoResponse => {

    console.log(`\n<Echo ${echoResponse.sender}> ${echoResponse.message}`);

    rl.prompt();

  });

  // replicate this set to successor
  peer.on('successor::up', successor => {

    // replicate to new successor

    //peer.replicateTo(successor);


  });

  peer.on('successor::down', successor => {

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

      case 'ping':

        var address = args._[0];

        var t = process.hrtime();

        if (!dht.Peer.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          try {

            await dht.ping(peer, address);

            t = process.hrtime(t);

            console.log(`<Ping ${address}> ${(t[1] / 1e6).toPrecision(3)} ms`);

          } catch (e) {
            
            console.error(e);

          }

          rl.prompt();

        })();

        break;

      case 'echo':

        var address = args._[0];

        var message = args._.slice(1).join(' ');

        if (!dht.Peer.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          try {
            
            await dht.echo(peer, address, message);

          } catch (e) {
          
            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();

        break;

      case 'join':
        
        var address = args._[0];

        if (!dht.Peer.isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          try {

            await dht.join(peer, address);

          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();
        
        break;

      case 'leave':

        (async () => {

          try {

            await dht.leave(peer);

          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();

        break;

      case 'get':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          try {

            let getResponse = await dht.get(peer, key);

            let value = getResponse.value.toString('utf8');

            console.log(`<Entry ${key}> ${value}`);
          
          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();

        break;

      case 'set':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }

        // TODO
        // - option to preserve white space (_.compact above removes it)

        let value = Buffer.from(args._.slice(1).join(' '));

        (async () => {

          try {

           await dht.set(peer, key, value);
          
          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();
      
        rl.prompt();
        
        break;

      case 'delete':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
        }
          
        (async () => {

          try {

            await dht.delete(peer, key);
          
          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();

        break;

      case 'quit':

        (async () => {

          try {

            await dht.leave(peer);
          
          } catch (e) {

            console.error(e);

          } finally {

            process.exit(0);
          
          }

        })();

        break;

      case 'info':
        
        printInfo(peer);

        rl.prompt();
        
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
