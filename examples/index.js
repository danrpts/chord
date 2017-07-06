'use strict'

const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const Bucket = require('./bucket.js').Bucket
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

function printInfo (bucket) {

  var info = '';
  info += `<Predecessor> ${(bucket.predecessor) ? bucket.predecessor : ''}\n`;
  info += `<Self> ${bucket.address}\n`;
  
  var successor = bucket.successor;
  for (let i in successor) {
    info += `<Successor ${i}> ${successor[i]}\n`;
  }

  for (let idStr in bucket.hashtable) {
    info += `<Entry ${idStr}> ${bucket.hashtable[idStr].toString('utf8')}\n`;
  }

  var finger = bucket.finger
  for (let i in finger) {
    info += `<Finger ${i}> ${finger[i]}\n`;
  }

  console.log(info);

}

function printHelp () {

  console.error('Commands:');
  console.error(' join <address>            Add bucket to network.');
  console.error(' leave                     Remove bucket from network.');
  console.error(' get <key>                 Read key from network.');
  console.error(' set <key> [value]         Create/update key in network.');
  console.error(' del <key>                 Delete key and value.');
  console.error(' ping <address>            Ping remote bucket.');
  console.error(' echo <address> [message]  Print to remote bucket.');
  console.error(' quit                      Leave network and exit.');
  console.error(' info                      Print info about bucket.');
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
  ||  _.has(argv, 'p') && !isPort(argv.p) // invalid port number
  ||  _.has(argv, 'r') && !_.isNumber(argv.r) // invalid successor/replica count
  ||  _.has(argv, 'join') && !isAddress(argv.join)) { // invalid address

  // TODO
  // - check invalid options

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  // TODO
  // - bucket uses random port when 0

  const bucket = new Bucket(argv.p, argv.r);
  bucket.start();

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "> "
  });

  if (argv.join) {
    bucket.join(argv.join);
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

        if (!isAddress(address)) {
          printHelp();
          rl.prompt();
          break;
        }

        (async () => {

          try {

            await bucket.ping(address);

            t = process.hrtime(t);

            console.log(`<Ping ${address}> ${(t[1] / 1e6).toPrecision(3)} ms`);

          } catch (e) {
            
            console.error(e);

          }

          rl.prompt();

        })();

        break;

      case 'join':
        
        var address = args._[0];

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
        
        break;

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

            let getResponse = await bucket.get(key);

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

           await bucket.set(key, value);
          
          } catch (e) {

            console.error(e);

          } finally {

            rl.prompt();
          
          }

        })();
      
        rl.prompt();
        
        break;

      case 'del':

        var key = args._[0];

        if (!_.isString(key)) {
          printHelp();
          rl.prompt();
          break;
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

        break;

      case 'quit':

        (async () => {

          try {

            await bucket.stop();
          
          } catch (e) {

            console.error(e);

          } finally {

            process.exit(0);
          
          }

        })();

        break;

      case 'info':
        
        printInfo(bucket);

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
