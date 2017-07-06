'use strict'

const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const Bucket = require('../lib/bucket.js').Bucket
const utils = require('../lib/utils.js');
const isPort = utils.isPort
const isAddress = utils.isAddress
const arg0 = 'chord'
const argv = minimist(process.argv.slice(2));

argv._ = _.compact(argv._);

function printUsage () {

  var spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [-p <port>]`);
  console.error(`       ${spacer} [-r <replicas>] [--join=<address>]\n`);

}

function printState (bucket) {

  var info = '';
  info += `<Predecessor> ${(bucket.predecessor) ? bucket.predecessor : ''}\n`;
  info += `<Self> ${bucket.address}\n`;
  
  var successor = bucket.successor;
  for (let i in successor) {
    info += `<Successor ${i}> ${successor[i]}\n`;
  }

  var finger = bucket.finger
  for (let i in finger) {
    info += `<Finger ${i}> ${finger[i]}\n`;
  }

  console.error(info);

}

function printBucket (bucket) {

  var info = '';
  for (let idStr in bucket.hashtable) {
    info += `<Entry ${idStr}> ${bucket.hashtable[idStr].toString('utf8')}\n`;
  }

  console.error(info);

}

function printHelp () {

  console.error('Commands:');
  console.error(' join <address>            Add this peer to a network.');
  console.error(' leave                     Remove this peer from the network.');
  console.error(' get <key>                 Read a value from network.');
  console.error(' set <key> [value]         Create/update a key in the network.');
  console.error(' del <key>                 Delete a key and value from the network.');
  console.error(' ping <address>            Ping a remote peer.');
  console.error(' state                     Print info about this peer.');
  console.error(' bucket                    Print bucket contents.')
  console.error(' quit                      Leave the network and exit.');
  console.error(' help                      Show this screen.\n');

}

if (argv.version) {

  console.log(require('../package.json').version);
  
  process.exit(0);

} else if (argv.help) {

  printUsage();

  printHelp();

  process.exit(0);

}

// check for invalid arguments
if (argv._.length > 0

  // invalid port number
  ||  _.has(argv, 'p') && (argv.p != 0) && !isPort(argv.p)

  // invalid successor count
  ||  _.has(argv, 'r') && !_.isNumber(argv.r) 

  // invalid host address
  ||  _.has(argv, 'join') && !isAddress(argv.join)) {

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  const bucket = new Bucket({
    nSuccessors: argv.r
  });

  try {
    bucket.listen(argv.p);
  } catch (e) {
    console.error(e);
    process.exit(1);  
  } 

  // define command interface
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "> "
  });

  // check for join argument
  if (isAddress(argv.join)) {
    bucket.join(argv.join);
  }

  rl.prompt();

  rl.on('line', (line) => {

    if (!line) {
      rl.prompt();
      return
    }

    let [command, ...args] = line.trim().split(' ');
    args = minimist(args);
    args._ = _.compact(args._);

    // TODO
    // handle empty line

    switch (command) {

      case 'ping':

        var address = args._[0]
        var t = process.hrtime();

        if (!isAddress(address)) {
          printHelp();
          rl.prompt();
          break
        }

        (async () => {
          try {
            await bucket.ping(address);
            t = process.hrtime(t);
            console.log(`<Ping ${address}> ${(t[1] / 1e6).toPrecision(3)} ms`);
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();
          }
        })();
        break

      case 'join':
        
        var address = args._[0]

        if (!isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {
          try {
            await bucket.join(address);
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();
          }
        })();
        break

      case 'leave':

        (async () => {
          try {
            await bucket.leave();
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();          
          }
        })();
        break

      case 'get':

        var key = args._[0]

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break
        }

        (async () => {
          try {
            let value = await bucket.get(key);
            console.log(`<Entry ${key}> ${value.toString('utf8')}`);
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();
          }
        })();
        break

      case 'set':

        var key = args._[0]
        var value = Buffer.from(args._.slice(1).join(' '));

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break
        }

        (async () => {
          try {
           await bucket.set(key, value);
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();
          }
        })();
        break

      case 'del':

        var key = args._[0]

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break
        }
          
        (async () => {
          try {
            await bucket.del(key);
          } catch (e) {
            console.error(e);
          } finally {
            rl.prompt();
          }
        })();
        break

      case 'state':
        
        printState(bucket);
        rl.prompt();
        break

      case 'bucket':
        
        printBucket(bucket);
        rl.prompt();
        break

      case 'quit':

        (async () => {
          try {
            bucket.close();
          } catch (e) {
            console.error(e);
          } finally {
            process.exit(0);
          }
        })();
        break

      case 'help':

        printHelp();
        rl.prompt();
        break

      default: 

        printHelp();
        rl.prompt();
        break
    
    }

  });

  rl.on('close', () => {
    process.exit(0);
  });

}
