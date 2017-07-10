'use strict'

const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const ip = require('ip').address();
const Bucket = require('../lib/bucket.js').Bucket;
var http = require('http').createServer();
var ioServer = require('socket.io')(http);
ioServer.listen = (port, ip) => new Promise((res) => {
  http.listen(port, ip, () => {
    ioServer.url = `${ip}:${http.address().port}`;
    res(ioServer.url);
  });
});
const utils = require('../lib/utils.js');
const isAddress = utils.isAddress;
const argv = minimist(process.argv.slice(2));
argv._ = _.compact(argv._);

(async () => {

  const bucket = new Bucket();

  try {
    
    await bucket.listen(argv.p || 0, ip);

    await ioServer.listen(0, ip);

    if (!isAddress(argv.join)) {
      throw new Error('Invalid host');
    }
    
    await bucket.join(argv.join);

    if (!_.isString(argv.nick)
      || argv.nick === ''
      || await bucket.has(argv.nick)) {
      throw new Error('Invalid nick');
    }

    await bucket.set(argv.nick, ioServer.url);

  } catch (e) {
    console.error(e);
    process.exit(1); 
  }

  ioServer.on('connection', socket => {
    socket.on('tell', object => {
      process.stdout.clearLine();
      process.stdout.cursorTo(0);
      console.log(`<${object.from}> ${object.message}`);
      rl.prompt(true);
    });
  });

  // define command interface
  const rl = readline.createInterface(process.stdin, process.stdout);

  rl.prompt();

  rl.on('line', line => {

    if (!line) {
      rl.prompt();
      return;
    }

    const me = argv.nick;
    let [cmd, ...args] = line.trim().split(' ');
    args = minimist(args);
    args._ = _.compact(args._);

    switch (cmd) {

      case '/tell':
        (async () => {
          const friend = args._[0];
          if (!_.isString(friend)) {
            return;
          }
          const ioClient = require('socket.io-client');
          var host;
          try {
            host = await bucket.get(friend);
          } catch (e) {
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            console.log(`${friend} is offline`);
            rl.prompt(true);
            return;
          }
          host = host.toString('utf8');
          if (!isAddress(host)) {
            process.stdout.clearLine();
            process.stdout.cursorTo(0);
            rl.prompt(true);
            return;
          }
          const socket = ioClient(`http://${host}`);
          const message = args._.slice(1).join(' ');
          socket.emit('tell', { from: me, message });
          rl.prompt();
          return;
        })();
        break;

      default: 
        rl.prompt();
        break;
    
    }

  });

  rl.on('close', () => {
    process.exit(0);
  });

})();
