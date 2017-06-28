const _ = require('underscore');
const minimist = require('minimist');
const readline = require('readline');
const dht = require('./dht.js');
const arg0 = 'chord';
const argv = minimist(process.argv.slice(2));


function printUsage () {

  var spacer = ' '.repeat(arg0.length);
  console.error(`Usage: ${arg0} [--version] [--help] [--fingers=<m>]`);
  console.error(`       ${spacer} [--successors=<r>] [-k <count> | --replicas=<k>]`);
  console.error(`       ${spacer} [-p <port>] [--join=<address>]`);

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
  console.error('join <address>                    Add peer to network.');
  console.error('leave                             Remove peer from network.');
  console.error('get <key> [-e <encoding>]         Read with encoding. (Default utf8)');
  console.error('set <key> <value> [-e <encoding>] Create/update with encoding. (Default utf8)');
  console.error('delete <key>                      Delete key and value.');
  console.error('ping <address>                    Ping remote peer.');
  console.error('echo <address> [message]          Print to remote peer.');
  console.error('info                              Print info about peer.');
  console.error('help                              Show this screen.');
  console.error('quit                              Shutdown peer and exit.');

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
||  _.has(argv, 'fingers') && !_.isNumber(argv.m) // invalid finger count
||  _.has(argv, 'successors') && !_.isNumber(argv.r) // invalid successor count
||  _.has(argv, 'k') && _.has(argv, 'replicas') // redundant option
||  _.has(argv, 'k') && !_.isNumber(argv.k) // invalid replica count
||  _.has(argv, 'replicas') && !_.isNumber(argv.replicas) // invalid replica count
||  _.has(argv, 'join') && !dht.isAddress(argv.join) // invalid address
) {

  // TODO
  // - check invalid options

  printUsage(arg0);

  process.exit(1);

// start cli
} else {

  // TODO
  // - peer uses random port when 0
  const port = argv.p || 0;

  const peer = dht.createPeer(port);

  peer.on('echo', getEcho => {

    console.log(`\n<Echo ${getEcho.addr}> ${getEcho.msg}`);

    rl.prompt();

  });

  // replicate this set to successor
  peer.on('replica', addrs => {

   //peer.setAll(getJoiningSuccessor.addr);

    console.log(`\n<Replica> ${addrs}`);

    rl.prompt();

  });

  // replicate successor set to this
  peer.on('removeReplica', (i, addr) => {

   //peer.getAll(getLeavingSuccessor.addr, { id: peer.id });

    console.log(`\n<Replica -${i}> ${addr}`);

    rl.prompt();

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

    // TODO
    // handle empty line

    switch (command) {

      case 'echo': 

        peer.echo(args[0], args.slice(1).join(' '));

        rl.prompt();

        break;

      case 'ping':
        
        var addr = args[0];
        
        (async () => {

          var pingResponse = await peer.ping(addr);
        
          console.log(`<Ping> ${addr} ${(pingResponse.dif.nans / 1e6).toPrecision(3)} ms`);          

          rl.prompt();

        })();

        break;

      case 'join':

        // TODO
        // - check !isJoined() 
        // - maybe use m,r,k params as network id hash

        args = minimist(args);
        
        peer.join(args._[0]);

        rl.prompt();
        
        break;

      case 'leave':

        // TODO
        // - check isJoined() 

        peer.leave();

        rl.prompt();

        break;

      case 'get':
                
        (async () => {
          
          var getResponse = await peer.get(args[0]);

          console.log(`GET ${getResponse.val}`);
          
          rl.prompt();

        })();

        break;

      case 'set':
          
        (async () => {

          var setResponse = await peer.set(args[0], args.slice(1).join(' '));
        
          //console.log(`SET ${args[0]} -> ${val}`); 

          rl.prompt();

        })();
        
        break;

      case 'delete':

        (async () => {
          
          var deleteResponse = await peer.delete(args[0]);

          console.log(`DELETE ${args[0]}`);
          
          rl.prompt();

        })();

        break;

      case 'info':
        
        printInfo(peer);

        rl.prompt();
        
        break;

      case 'quit':

        peer.close();

        process.exit(0);

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

        printUsage();

        rl.prompt();

        break;
    
    }

  });

  rl.on('close', () => {
    
    process.exit(0);

  });

}
