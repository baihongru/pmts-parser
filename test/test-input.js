const { program } = require('commander');
const redis = require('redis');
const Tail = require('tail').Tail;

// 命令行参数
program
.version('1.0.0')
.option('-rhost, --redis-host <ip>', 'Specify the Redis server IP address', '127.0.0.1')
.option('-rport, --redis-port <port>', 'Specify the Redis port', '6379')
.option('-ruser, --redis-user <username>', 'Specify the Redis username', '')
.option('-rpass, --redis-pass <password>', 'Specify the Redis password', '')
.option('-rdb, --redis-db <db>', 'Specify the Redis DB', '0')
.requiredOption('-rpub, --redis-pub <channel name>', 'Specify the Redis publish channel')
.requiredOption('-f, --file <file name>', 'Specifies the absolute path to the file');

program.parse(process.argv);

const cmdOptions = program.opts();
console.log('Startup parameter is:\n', cmdOptions);

// Redis客户端
const redisUrl = `redis://${(cmdOptions.redisUser && cmdOptions.redisPass) ? cmdOptions.redisUser + ':' + cmdOptions.redisPass + '@' : ''}${cmdOptions.redisHost}:${cmdOptions.redisPort}`;
const client = redis.createClient(redisUrl, {
    'db': cmdOptions.redisDb
});

client.on('error', (err) => {
    console.log(`Redis client error is:\n${err}`);
});

const tailOption = {'fromBeginning': true};
const tail = new Tail(cmdOptions.file, tailOption);

tail.on('line', (data) => {
    client.publish(cmdOptions.redisPub, JSON.stringify({'message': data}));
});

// 启动命令示例
// node test-input.js -rpub pmts-01 -f /home/voyager/dev/PMTS_Parser/test/log_sample/success.msg.000001.pmts01
