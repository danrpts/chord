const minimist = require('minimist');
const readline = require('readline');
const dht = require('./dht.js');

function printUsage () {
  console.log('Usage: chord [--version] [--help]\n',
              '            <command> [<args>]\n');
}

function printHelp () {
  console.log('Commands are:\n\n',
              'on [-p <port>]              Enable this node\n',
              'off                         Disable this node\n',
              'ping <address>              Ping remote node\n',
              'join <address>              Add this node to a network\n',
              'leave                       Remove this node from network\n',
              'get <key>                   Find value in network\n',
              'set <key> <value>           Update key in network\n',
              'clear                       Clear the terminal screen\n',
              'quit                        Exit shell immediately\n',
              'help                        Display command help');
}


// TODO
// change slice later
// note the let
// input args from node command
var argv = minimist(process.argv.slice(2));

if (argv.version) {
  console.log(require('./package.json').version);
  process.exit(0);
} else if (argv.help) {
  printUsage();
  printHelp();
  process.exit(0);
}

// begin cli
var peer = null;
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: "chord> "
});

rl.prompt();

rl.on('line', (line) => {

  if (!line) {
    rl.prompt();
    return;
  }

  var [cmd, ...args] = line.trim().split(' ');

  // TODO
  // handle empty line

  switch (cmd) {

    case 'on':

      args = minimist(args);

      var port = parseInt(args.p);

      if (!args.p) {

        printHelp();

        rl.prompt();

        return;

      // TODO
      // check what GRPC uses as transport protocol
      // and if allowable lower port space
      // user supplied port (create node)
      } else if (1024 > port || port > 65535) {

        console.log('CHORD_INVALID_PORT');

        rl.prompt();

        return;

      }
        
      peer = new dht.Peer(port);

      console.log(`UP ${peer.addr} (${peer.id})`);

      rl.prompt();

      break;

    case 'ping':
      
      var addr = args[0];
      
      (async () => {

        var t;

        try {
        
          t = await peer.ping(addr);
        
          console.log(`PING ${addr} ${(t.dif.nans / 1e6).toPrecision(3)} ms`);
        
        } catch (e) {
          
          console.log('CHORD_PING_ERROR', e);

        }

        rl.prompt();

      })();

      break;

    case 'join':
      
      var addr = args[0];
      
      (async () => {

        var succ;

        try {
        
          succ = await peer.join(addr);
        
          console.log(`JOIN ${succ.addr}`);
        
        } catch (e) {
          
          console.log('CHORD_JOIN_ERROR', e);

        }

        rl.prompt();

      })();
      
      break;

    case 'info':

      if (peer) {
      
        console.log(peer.toString());
        
      }

      rl.prompt();
      
      break;
     
    case 'clear':

      process.stdout.write('\033c');
      
      rl.prompt();

      break;

    case 'quit':

      if (peer) {
      
        peer.shutdown();
        
      }

      process.exit(0);

      break;

    case 'help':

      printHelp();

      rl.prompt();

      break;

    default: 

      console.log('CHORD_INVALID_COMMAND');

      printHelp();

      rl.prompt();

      break;
  
  }

});
rl.on('close', () => {
  process.exit(0);
});

if (argv._.length) rl.write(process.argv.slice(2).join(' ') + '\n');

