const minimist = require('minimist');
const readline = require('readline');
const dht = require('./dht.js');

function printUsage () {
  console.log('Usage: dht [-q] [-4] [-6] [-i filename] [-p address port]...\n',
              '           port [address port]...\n');
}

function printHelp () {
    console.log('Commands may be abbreviated.  Commands are:\n\n',
                'ping     ping a ring node\n',
                'dump     display node info\n',
                'quit     exit ring shell\n',
                'new      create a new ring network\n',
                'join     join a ring network\n',
                'show     show joined ring networks\n',
                'leave    leave a ring network\n',
                'get      get value from a ring network\n',
                'set      set value in a ring network\n');
}

function main () {

  var argv = minimist(process.argv.slice(2));

  var port = argv.p || argv.port;

  var peer = new dht.Peer(port);  

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: "ring> "
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

      case 'help':

        printHelp();

        rl.prompt();

        break;

      case 'quit':

        process.exit(0);

        break;

      case 'ping':
        
        var address = args[0];
        
        peer.ping(address, () => {
          rl.prompt();
        });
        
        break;

      case 'join':
        
        var address = args[0];
        
        peer.join(address, () => {

          rl.prompt();
          
        });
        
        break;

      case 'get':
        
        peer.get(args[0], (err, res) => {

          if (err) {
            console.log(err);
          } else {
            console.log(`GET ${res.hash.toString('hex')}: ${res.value.toString()}`);
          }

          rl.prompt();

        });
        
        break;

      case 'set':

        peer.set(args[0], args[1], (err, res) => {
          
          if (err) {
            console.log(err);
          } else {
            console.log(`SET ${res.hash.toString('hex')}`);
          }

          rl.prompt();

        });
        
        break;

      case 'dump':
        
        peer.dump();
          
        rl.prompt();
        
        break;

      default: 

        console.log('Invalid command');

        rl.prompt();

        break;
    }

  }).on('close', () => {

    process.exit(0);

  });

}

main();