const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const ip = require('ip');
const Bucket = require('../lib/bucket.js').Bucket;
const utils = require('../lib/utils.js');
const version = require('../package.json').version;

const isPort = utils.isPort;
const isAddress = utils.isAddress;
const arg0 = 'chord';

// parse command line arguments
const argv = minimist(process.argv.slice(2));

// remove redundant white space
argv._ = _.compact(argv._);

function printUsage() {
  const spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [-p <port>]`);
  console.error(`       ${spacer} [-r <replicas>] [--join=<address>]\n`);
}

function printHelp() {
  console.error('Commands:');
  console.error(' join <address>            Add this peer to a network.');
  console.error(' get <key>                 Read a value from network.');
  console.error(' set <key> [value]         Create/update a key in the network.');
  console.error(' del <key>                 Delete a key and value from the network.');
  console.error(' ping <address>            Ping a remote peer.');
  console.error(' state [address] [-f]      Print state information.');
  console.error(' dump [address]            Print bucket contents.');
  console.error(' quit                      Leave the network and exit.');
  console.error(' help                      Show this screen.\n');
}

if (argv.version) {
  console.log(version);

  process.exit(0);
} else if (argv.help) {
  printUsage();

  printHelp();

  process.exit(0);
}

// check for invalid arguments
if (argv._.length > 0

  // invalid port number (use 0 for random port)
  || (_.has(argv, 'p') && (argv.p !== 0) && !isPort(argv.p))

  // invalid successor count
  || (_.has(argv, 'r') && !_.isNumber(argv.r))

  // invalid host address
  || (_.has(argv, 'join') && !isAddress(argv.join))) {
  printUsage(arg0);

  process.exit(1);

// start cli
} else {
  (async () => {
    let host;
    let key;
    let value;

    const rl = readline.createInterface(process.stdin, process.stdout);

    const bucket = new Bucket({
      nSuccessors: argv.r,
    });

    bucket.on('predecessor::up', (up) => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`predecessor::up ${up}`);
      rl.prompt(true);
    });

    bucket.on('predecessor::down', (down) => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`predecessor::down ${down}`);
      rl.prompt(true);
    });

    bucket.on('successor::up', (up) => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`successor::up ${up}`);
      rl.prompt(true);
    });

    bucket.on('successor::down', (down) => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`successor::down ${down}`);
      rl.prompt(true);
    });

    try {
      await bucket.listen(argv.p, ip.address());
      console.log(`${bucket.id.toString('hex')} on ${bucket.address}`);
      if (_.has(argv, 'join')) {
        await bucket.join(argv.join);
      }
    } catch (e) {
      console.error(e);
      process.exit(1);
    }

    rl.prompt();

    rl.on('close', () => {
      process.exit(0);
    });

    rl.on('line', (line) => {
      if (!line) {
        rl.prompt();
        return;
      }

      const [command, ...commandArgs] = line.trim().split(' ');
      const parsedCommandArgs = minimist(commandArgs);
      parsedCommandArgs._ = _.compact(parsedCommandArgs._);

      // TODO
      // handle empty line

      switch (command) {

        case 'ping':

          host = parsedCommandArgs._[0];

          if (!isAddress(host)) {
            printHelp();
            rl.prompt();
            break;
          }

          (async () => {
            try {
              const t = await bucket.ping(host);
              console.log(`<Ping ${host}> ${(t[1] / 1e6).toPrecision(3)} ms`);
            } catch (e) {
              console.error(e);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'join':

          host = parsedCommandArgs._[0];

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

          key = parsedCommandArgs._[0];

          if (!_.isString(key)) {
            printHelp();
            rl.prompt();
            break;
          }

          (async () => {
            try {
              value = await bucket.get(key);
              console.log(`<Entry ${key}> ${value.toString('utf8')}`);
            } catch (__) {
              console.error(`<Entry ${key}> undefined`);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'set':

          key = parsedCommandArgs._[0];
          value = Buffer.from(parsedCommandArgs._.slice(1).join(' '));

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

          key = parsedCommandArgs._[0];

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

          host = parsedCommandArgs._[0];

          // print local
          if (!isAddress(host)) {
            host = undefined;
          }

          (async () => {
            try {
              const response = await bucket.state(host, parsedCommandArgs.f);
              console.log(`<Predecessor> ${(response.predecessor) ? response.predecessor : ''}`);
              console.log(`<Self> ${response.address}`);
              response.successor.forEach((successor, i) => {
                console.log(`<Successor ${i}> ${successor}`);
              });
              if (parsedCommandArgs.f) {
                response.finger.forEach((finger, i) => {
                  console.log(`<Finger ${i}> ${finger}`);
                });
              }
            } catch (e) {
              console.error(e);
            } finally {
              rl.prompt();
            }
          })();
          break;

        case 'dump':

          host = parsedCommandArgs._[0];

          // print local
          if (!isAddress(host)) {
            host = undefined;
          }

          (async () => {
            try {
              const entries = await bucket.dump(host);
              Object.keys(entries).forEach((entryKey) => {
                console.log(`<Entry ${entryKey}> ${entries[entryKey].toString('utf8')}`);
              });
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
