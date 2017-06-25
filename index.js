const minimist = require('minimist');
const readline = require('readline');
const getPort = require('get-port');
const dht = require('./dht.js');

function printUsage () {
  console.log('Usage: chord [--version] [--help]\n',
              '            <command> [<args>]\n');
}

function printHelp () {
  console.log('Commands are:\n\n',
              'create [-p <port>]          Initialize this node\n',
              'echo <address> [arg ...]    Output to remote node\n',
              'ping <address>              Ping remote node\n',
              'join <address>              Add this node to a network\n',
              'leave                       Remove this node from network\n',
              'get <key>                   Find value in network\n',
              'set <key> <val ...>         Update key in network\n',
              'info                        Output info about this node\n',
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

  prompt: "# "

});

rl.prompt();

rl.on('line', (line) => {

  if (!line) {

    rl.prompt();

    return;

  }

  let [cmd, ...args] = line.trim().split(' ');

  // TODO
  // handle empty line

  switch (cmd) {

    case 'create':

      var port;

      args = minimist(args);

      port = args.p;

      (async () => {

        if (peer) {

            try {
              
              var res = await peer.shutdown();

              console.log(`SHUTDOWN ${res.addr} (${res.id.toString('hex')})`);

              peer = undefined;

            } catch (err) {

              console.log('create::shutdown', err);

            }

        }

        try {

          port = await getPort(port);

          peer = new dht.Peer(port);

          peer.on('echo', res => {

            console.log(`\nECHO ${res.addr} (${res.msg})`);

            rl.prompt();

          });

          console.log(`CREATE ${peer.addr} (${peer.id.toString('hex')})`);


        } catch (err) {

          console.log(err);

        }

        rl.prompt();

      })();

      break;

    case 'echo': 

     if (!peer) {

        console.log('node uninitialized');

      } else {
      
        var addr = args[0];

        var msg = args.slice(1).join(' ');

        peer.echo(addr, msg);

      }   

      rl.prompt();

      break;

    case 'ping':

      if (!peer) {

        console.log('node uninitialized');
      
        rl.prompt();

      } else {
      
        var addr = args[0];
        
        (async () => {

          var res = await peer.ping(addr);
        
          console.log(`PING ${addr} ${(res.dif.nans / 1e6).toPrecision(3)} ms`);          

          rl.prompt();

        })();

      }

      break;

    case 'join':

      if (!peer) {

        console.log('node uninitialized');

      } else {

        args = minimist(args);
        
        peer.join(args._[0]);
          
      }

      rl.prompt();
      
      break;

    case 'leave':

      // TODO

      rl.prompt();
      
      break;

    case 'get':

      if (!peer) {

        console.log('node uninitialized');

        rl.prompt();

      } else {
              
        (async () => {
          
          var res = await peer.get(args[0]);

          console.log(`GET ${res.val}`);
          
          rl.prompt();

        })();

      }

      break;

    case 'set':

      if (!peer) {

        console.log('node uninitialized');

        rl.prompt();

      } else {
        
        (async () => {

          var val = args.slice(1).join(' ');

          var res = await peer.set(args[0], val);
        
          //console.log(`SET ${args[0]} -> ${val}`); 

          rl.prompt();

        })();

      }
      
      break;

    case 'info':

      if (!peer) {

        console.log('node uninitialized');

      } else {
      
        console.log(peer.toString());
        
      }

      rl.prompt();
      
      break;
     
    case 'clear':

      process.stdout.write('\033c');
      
      rl.prompt();

      break;

    case 'pause':

      clearInterval(peer.timeout);
      
      rl.prompt();

      break;

    case 'quit':

      if (!peer) {

        console.log('node uninitialized');

        process.exit(0);

      } else {

        (async () => {

          try {
            
            var res = await peer.shutdown();

            console.log(`SHUTDOWN ${res.addr} (${res.id.toString('hex')})`);

            peer = undefined;

          } catch (err) {

            console.log('quit::shutdown', err);

          }

          process.exit(0);

        })();

      }

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

if (argv._.length) rl.write(process.argv.slice(2).join(' ') + '\n');

