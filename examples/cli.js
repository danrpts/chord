'use strict'

const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const ip = require('ip');
const Bucket = require('../lib/bucket.js').Bucket;
const utils = require('../lib/utils.js');
const isPort = utils.isPort;
const isAddress = utils.isAddress;
const toStateString = utils.toStateString;
const arg0 = 'chord';

// parse command line arguments
const argv = minimist(process.argv.slice(2));

// remove redundant white space
argv._ = _.compact(argv._);

function printUsage () {

  var spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [-p <port>]`);
  console.error(`       ${spacer} [-r <replicas>] [--join=<address>]\n`);

}

function printBucket (contents) {

  var info = '';
  for (let key in contents) {
    info += `<Entry ${key}> ${contents[key].toString('utf8')}\n`;
  }

  console.error(info);

}

function printHelp () {

  console.error('Commands:');
  console.error(' join <address>            Add this peer to a network.');
  console.error(' get <key>                 Read a value from network.');
  console.error(' set <key> [value]         Create/update a key in the network.');
  console.error(' del <key>                 Delete a key and value from the network.');
  console.error(' ping <address>            Ping a remote peer.');
  console.error(' state [address] [-f]      Print state information.');
  console.error(' bucket [address]          Print bucket contents.')
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

  // invalid port number (use 0 for random port)
  ||  _.has(argv, 'p') && (argv.p != 0) && !isPort(argv.p)

  // invalid successor count
  ||  _.has(argv, 'r') && !_.isNumber(argv.r) 

  // invalid host address
  ||  _.has(argv, 'join') && !isAddress(argv.join)) {

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  (async () => {

    const bucket = new Bucket({
      nSuccessors: argv.r
    });

    bucket.on('successor::up', up => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`UP ${up}`);
      rl.prompt(true);
    });

    bucket.on('successor::down', down => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`DOWN ${down}`);
      rl.prompt(true);
    });

    try {
      await bucket.listen(argv.p, ip.address());
      if (_.has(argv, 'join')) {
        await bucket.join(argv.join);
      }
    } catch (e) {
      console.error(e);
      process.exit(1);  
    }

    // define command interface
    const rl = readline.createInterface(process.stdin, process.stdout);

    rl.prompt();

    rl.on('close', () => {
      process.exit(0);
    });

    rl.on('line', line => {

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

          var host = args._[0];
          var t = process.hrtime();

          if (!isAddress(host)) {
            printHelp();
            rl.prompt();
            break;
          }

          (async () => {
            try {
              let t = await bucket.ping(host);
              console.log(`<Ping ${host}> ${(t[1] / 1e6).toPrecision(3)} ms`);
            } catch (e) {
              console.error(e);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'join':
          
          var host = args._[0];

          if (!isAddress(host)) {
            printHelp();
            rl.prompt();
            break;
          }

          (async () => {
            try {
              await bucket.join(host);
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
              let value = await bucket.get(key);
              console.log(`<Entry ${key}> ${value.toString('utf8')}`);
            } catch (_) {
              console.error(`<Entry ${key}> undefined`);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'set':

          var key = args._[0];
          var value = Buffer.from(args._.slice(1).join(' '));

          if (!_.isString(key)) {
            printHelp();
            rl.prompt();
            break;
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

        case 'state':
          
          var host = args._[0];

          // print local
          if (!isAddress(host)) {
            let state = toStateString(bucket, args.f);
            console.log(state);
            rl.prompt();
            break;
          }
          // print remote
          (async () => {
            try {
              let response = await bucket.state(host, args.f);
              response.address = host;
              let state = toStateString(response);
              console.log(state);
            } catch (e) {
              console.error(e);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'bucket':

          var host = args._[0];

          // print local
          if (!isAddress(host)) {
            printBucket(bucket.hashtable);
            rl.prompt();
            break;
          }

          // print remote
          (async () => {
            try {
              let contents = await bucket.all(host);
              printBucket(contents);
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
              bucket.close();
            } catch (e) {
              console.error(e);
            } finally {
              process.exit(0);
            }
          })();
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

  })();

}
