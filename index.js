const minimist = require('minimist');
const readline = require('readline');
const dht = require('./dht.js');

function printUsage () {
  console.log('Usage: chord [--version] [--help]\n',
              '            <command> [<args>]\n');
}

function printInfo (peer) {
  var info = '';
  info += ` <Predecessor> ${(peer.predecessor) ? peer.predecessor : ''}\n`;
  info += ` <Self> ${peer.addr}\n`;
  
  var successorList = peer.successorList;
  for (let i in peer.successorList) {
    info += ` <Successor ${i}> ${successorList[i]}\n`;
  }

  for (let idStr in peer.bucket) {
    info += ` <Bucket ${idStr}> ${peer.bucket[idStr].toString('utf8')}\n`;
  }

  var finger = peer.finger
  for (let i in finger) {
    info += ` <Finger ${i}> ${finger[i]}\n`;
  }

  console.log(info);
}

function printHelp () {
  console.log('Commands are:\n\n',
              'create [port]               Initialize this node\n',
              'echo <address> [arg ...]    Output to remote node\n',
              'ping <address>              Ping remote node\n',
              'join <address>              Add this node to a network\n',
              'leave                       Remove this node from network\n',
              'get <key>                   Find value in network\n', // read
              'set <key> <value ...>       Update key in network\n', // create, update
              'delete <key>                Delete key in network\n', // delete
              'info                        Output info about this node\n',
              'quit                        Exit CLI immediately\n',
              'clear                       Clear the terminal screen\n',
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

  prompt: "> "

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

      if (peer) {

        peer.close();

        peer.removeListener('echo');

        peer = undefined;

        rl.prompt();

      } else {

        (async () => {

          peer = dht.createPeer(args[0]);

          peer.on('echo', getEcho => {

            console.log(`\nECHO ${getEcho.addr} (${getEcho.msg})`);

            rl.prompt();

          });

          console.log(`CREATE ${peer.addr} (${peer.id.toString('hex')})`);

          rl.prompt();

        })();

      }

      break;

    case 'echo': 

     if (!peer) {

        console.log('node uninitialized');

      } else {

        peer.echo(args[0], args.slice(1).join(' '));

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

          var pingResponse = await peer.ping(addr);
        
          console.log(`PING ${addr} ${(pingResponse.dif.nans / 1e6).toPrecision(3)} ms`);          

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

      if (!peer) {

        console.log('node uninitialized');

      } else {

        peer.leave();

      }

      rl.prompt();

      break;

    case 'get':

      if (!peer) {

        console.log('node uninitialized');

        rl.prompt();

      } else {
              
        (async () => {
          
          var getResponse = await peer.get(args[0]);

          console.log(`GET ${getResponse.val}`);
          
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

          var setResponse = await peer.set(args[0], args.slice(1).join(' '));
        
          //console.log(`SET ${args[0]} -> ${val}`); 

          rl.prompt();

        })();

      }
      
      break;

    case 'delete':

      if (!peer) {

        console.log('node uninitialized');

        rl.prompt();

      } else {
              
        (async () => {
          
          var deleteResponse = await peer.delete(args[0]);

          console.log(`DELETE ${args[0]}`);
          
          rl.prompt();

        })();

      }

      break;

    case 'info':

      if (!peer) {

        console.log('node uninitialized');

      } else {
      
        printInfo(peer);
        
      }

      rl.prompt();
      
      break;

    case 'quit':

      if (!peer) {

        process.exit(0);

      } else {

        (async () => {

          peer.close();

          peer.removeListener('echo');

          peer = undefined;

          process.exit(0);

        })();

      }

      break;
     
    case 'clear':

      process.stdout.write('\033c');
      
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

if (argv._.length) rl.write(process.argv.slice(2).join(' ') + '\n');

