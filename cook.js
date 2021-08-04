const { program } = require('commander');
const redis = require('redis');
const log4js = require('log4js');
const fs = require('fs');

// 命令行参数
program
.version('1.0.0')
.option('-rhost, --redis-host <ip>', 'Specify the Redis server IP address', '127.0.0.1')
.option('-rport, --redis-port <port>', 'Specify the Redis port', '6379')
.option('-ruser, --redis-user <username>', 'Specify the Redis username', '')
.option('-rpass, --redis-pass <password>', 'Specify the Redis password', '')
.option('-rdb, --redis-db <db>', 'Specify the Redis DB', '0')
.option('-tkprefix, --trans-key-prefix <prefix>', 'Specify the Redis DB', 'pmts_transaction:')
.option('-nkprefix, --notice-key-prefix <prefix>', 'Specify the Redis DB', 'pmts_notice:')
.requiredOption('-rpub, --redis-pub <channel name>', 'Specify the Redis publish channel');

program.parse(process.argv);
const cmdOptions = program.opts();

let log4jsConf = JSON.parse(fs.readFileSync('config/cook-log4js.json'));
log4jsConf.appenders.cook.filename = `logs/cook-${cmdOptions.redisPub}.log`;

log4js.configure(log4jsConf);

const logger = log4js.getLogger('cook');

logger.info('Startup parameter:\n', cmdOptions);
logger.info('Process PID:', process.pid);

// Redis客户端
const redisUrl = `redis://${(cmdOptions.redisUser && cmdOptions.redisPass) ? cmdOptions.redisUser + ':' + cmdOptions.redisPass + '@' : ''}${cmdOptions.redisHost}:${cmdOptions.redisPort}`;
const subClient = redis.createClient(redisUrl, {
    'db': cmdOptions.redisDb
});

subClient.on('error', (err) => {
    logger.error('subClient err:', err);
});

const otherClient = subClient.duplicate();

// 解析消息
// 解析前数据结构示例：
// {
//     '2021-02-28T00:57:40.186Z': '{"msgId":"0000000500002021021868623170","msgType":"saps.601.001.01","mesgDirection":"D","msgHeader":"{H:010000        IBPS313821001016IBPS20210218053015XMLsaps.601.001.01     IBPA6014CC8E22247821IBPA6014CC8E222478213D2        }"}',
//     '2021-02-28T00:57:40.177Z': '{"msgId":"1051000000172021021803754393","msgType":"ibps.101.001.02","mesgDirection":"D","msgHeader":"{H:01105100000017IBPS313821001016IBPS20210218053014XMLibps.101.001.02     20210218050561986146202102180505619861463D         }"}',
//     meta: '{"msgId":"1051000000172021021803754393","msgType":"ibps.101.001.02","mesgDirection":"D","msgHeader":"{H:01105100000017IBPS313821001016IBPS20210218053014XMLibps.101.001.02     20210218050561986146202102180505619861463D         }"}',
//     '2021-02-28T00:57:40.181Z': '{"msgId":"3138210010162021021873098137","msgType":"ibps.102.001.01","mesgDirection":"U","msgHeader":"{H:01313821001016IBPS105100000017IBPS20210218053015XMLibps.102.001.01     10162021021873098137101620210218730981373U       X }"}'
// }
// 解析后数据结构示例：
// {
//     meta: {
//         msgId: '1051000000172021021803754393',
//         msgType: 'ibps.101.001.02',
//         mesgDirection: 'D',
//         msgHeader: '{H:01105100000017IBPS313821001016IBPS20210218053014XMLibps.101.001.02     20210218050561986146202102180505619861463D         }'
//     },
//     msgSeq: [
//         {
//             timestamp: '2021-02-28T00:57:40.177Z',
//             body: '{"msgId":"1051000000172021021803754393","msgType":"ibps.101.001.02","mesgDirection":"D","msgHeader":"{H:01105100000017IBPS313821001016IBPS20210218053014XMLibps.101.001.02     20210218050561986146202102180505619861463D         }"}'
//         },
//         {
//             timestamp: '2021-02-28T00:57:40.181Z',
//             body: '{"msgId":"3138210010162021021873098137","msgType":"ibps.102.001.01","mesgDirection":"U","msgHeader":"{H:01313821001016IBPS105100000017IBPS20210218053015XMLibps.102.001.01     10162021021873098137101620210218730981373U       X }"}'
//         },
//         {
//             timestamp: '2021-02-28T00:57:40.186Z',
//             body: '{"msgId":"0000000500002021021868623170","msgType":"saps.601.001.01","mesgDirection":"D","msgHeader":"{H:010000        IBPS313821001016IBPS20210218053015XMLsaps.601.001.01     IBPA6014CC8E22247821IBPA6014CC8E222478213D2        }"}'
//         }
//     ],
//     durationSeq: [ 4, 5, 9 ]
// }

function parseMsg(msg) {
    let obj = {
        'meta': {},
        'msgSeq': [],
        'durationSeq': []
    };
    
    const msgMeta = JSON.parse(msg.meta);
    obj.meta['msgId'] = msgMeta['msgId'];
    obj.meta['msgType'] = msgMeta['msgType'];
    obj.meta['mesgDirection'] = msgMeta['mesgDirection'];
    obj.meta['msgHeader'] = msgMeta['msgHeader'];
    
    delete msg.meta;
    let msgKeys = [];
    msgKeys = Object.keys(msg);
    // 升序排列
    msgKeys.sort();

    obj.msgSeq = msgKeys.map((currentValue, index, arr) => {
        return {
            'timestamp': currentValue,
            'body': msg[currentValue]
        };
    });

    obj.durationSeq = msgKeys.map((currentValue, index, arr) => {
        if (index !== 0) {
            return new Date(arr[index]).getTime() - new Date(arr[index - 1]).getTime();
        }
    });
    obj.durationSeq.shift();
    // 当obj.durationSeq为空时返回undefined
    let totalDuration = eval(obj.durationSeq.join('+'));
    obj.durationSeq.push(totalDuration);
    //console.log(obj);
    return obj;
}

// 处理订阅消息
subClient.on('subscribe', (channel, count) => {
    logger.info(`Subscribe channel: ${channel}, Current total number of subscriptions: ${count}`);
});

let msgCount = 0;
const regMsgId = new RegExp(cmdOptions.noticeKeyPrefix + '(.*)');
subClient.on('message', (channel, message) => {
    // message为过期的key名称
    if (regMsgId.test(message)) {
        const transactionKey = cmdOptions.transKeyPrefix + message.match(regMsgId)[1];
        otherClient.hgetall(transactionKey, (err, value) => {
            if (!err) {
                try {
                    otherClient.publish(cmdOptions.redisPub, JSON.stringify(parseMsg(value)), (err, number) => {
                        if(!err) {
                            msgCount ++;
                            if (!Number.isSafeInteger(msgCount)) {
                                msgCount = 0;
                                logger.info('Message count is reset to 0');
                            }
                        }
                    });
                } catch (err) {
                    logger.error('Parse error:', err);
                }
            }
        });
    }
});

subClient.subscribe('__keyevent@' + cmdOptions.redisDb + '__:expired');

// 循环事件
let msgCountPrint = setInterval(() => {
    logger.info('Send message total:', msgCount);
}, 1000 * 60);

process.on('exit', (code) => {
    console.log('');
    log4js.shutdown(() => {});
});

function sigHandle(sig) {
    clearInterval(msgCountPrint);
    subClient.quit();
    otherClient.quit();
    logger.info('Process exit');
    setTimeout(() => {
        process.exit(0);
    }, 100);
}

// ctrl+l触发
process.on('SIGINT', sigHandle);
// kill触发
process.on('SIGTERM', sigHandle);

// 启动命令示例
// node cook.js -rpub pmts-out
