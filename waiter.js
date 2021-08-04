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
.requiredOption('-rsub, --redis-sub <channel name>', 'Specify the Redis subscription channel');

program.parse(process.argv);
const cmdOptions = program.opts();

let log4jsConf = JSON.parse(fs.readFileSync('config/waiter-log4js.json'));
log4jsConf.appenders.waiter.filename = `logs/waiter-${cmdOptions.redisSub}.log`;

log4js.configure(log4jsConf);

const logger = log4js.getLogger('waiter');

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

// 解析订阅消息
const statusSet = {
    "collect": 0, // 合并属于同一条消息的多行日志
    "filter": 1,  // 过滤不需要解析的消息类型
    "check": 2,   // 检查消息的合法性
    "parser": 3   // 提取交易元数据和读写redis
};
let status = statusSet.collect;
let msgCount = 0;
let msgBuf = '';

// 匹配新行：[2021-02-18 07:29:16.929538][5488][10971830] Level 0 PMTSMSGHDL:
const regCollectStart = /^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}\]\[\d{0,}\]\[\d{0,}\] Level 0 PMTSMSGHDL:\s{0,1}$/;
// 匹配包含：</Document>
const regCollectEnd = /<\/Document>/;

// 过滤不需要解析的消息类型
const filterList = [
    'ccms.990.001.01', // 通信级确认报文，版本1
    'ccms.990.001.02', // 通信级确认报文，版本2
    'ccms.911.001.01'  // 报文丢弃通知报文
];

function filterMsgAllow(msg) {
    let temp = true;
    filterList.forEach((item) => {
        temp = temp && !msg.includes(item);
    });
    // 返回true，保留消息
    return temp;
}

// 检查报文合法性
// 检查要素:
// 1. 有且只有一个消息开始标志
// 2. 有且只有一个报文头
// 3. 有且只有一个报文标识号字段：<MsgId>...</MsgId>
// 4. 有一个或零个原报文标识号字段：<OrgnlMsgId>...</OrgnlMsgId>或<Ntfctn><Id>...</Id>，两字段不能同时出现

const regMsgStart = /\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}\]\[\d{0,}\]\[\d{0,}\] Level 0 PMTSMSGHDL:/g;
// 已知两类通信级报文头，《网上支付跨行清算系统报文交换标准 1.6.4》只包含长度为122的类别
//const regMsgHeader = /\{H:[0-9a-zA-Z_\s\\.\\-]{122}\}|\{H:[0-9a-zA-Z_\s\\.\\-]{126}\}/g;
// 在通信级报文头中提取MesgType、MesgDirection字段
const regMsgHeader = /\{H:.{51}([0-9a-zA-Z_\s\\.\\-]{20}).{41}([DU]{1}).{9}\}|\{H:.{55}([0-9a-zA-Z_\s\\.\\-]{20}).{41}([DU]{1}).{9}\}/g;
const regMsgIdField = /<MsgId>([0-9a-zA-Z]{0,35})<\/MsgId>/g;
const regOrgnlMsgIdField = /<OrgnlMsgId>([0-9a-zA-Z]{0,35})<\/OrgnlMsgId>/g;
const regNtfctnIdField = /<Ntfctn><Id>([0-9a-zA-Z]{0,35})<\/Id>/g;

function checkMsgAllow(msg) {
    // 返回true，检查通过
    return (
        (msg.match(regMsgStart) || []).length === 1 &&
        (msg.match(regMsgHeader) || []).length === 1 &&
        (msg.match(regMsgIdField) || []).length === 1 &&
        (
            (msg.match(regOrgnlMsgIdField) || []).length === 0 && (msg.match(regNtfctnIdField) || []).length === 0 ||
            (msg.match(regOrgnlMsgIdField) || []).length === 1 && (msg.match(regNtfctnIdField) || []).length === 0 ||
            (msg.match(regOrgnlMsgIdField) || []).length === 0 && (msg.match(regNtfctnIdField) || []).length === 1
        )
    );
}

// 解析消息，获取交易元数据
const regMsgHeaderParse = new RegExp(regMsgHeader, '');
const regMsgIdFieldParse = new RegExp(regMsgIdField, '');
const regOrgnlMsgIdFieldParse = new RegExp(regOrgnlMsgIdField, '');
const regNtfctnFieldParse = new RegExp(regNtfctnIdField, '');

function parseMsg(msg) {
    let obj = {};
    const msgHeader = msg.match(regMsgHeaderParse);
    // 交易元数据：报文标识号
    obj['msgId']  = msg.match(regMsgIdFieldParse)[1].trim();
    // 交易元数据：报文类型
    obj['msgType'] = (msgHeader[1] || msgHeader[3]).trim();
    // 交易元数据：报文方向
    obj['mesgDirection'] = msgHeader[2] || msgHeader[4];
    // 交易元数据：报文头
    obj['msgHeader'] = msgHeader[0];
    return obj;
}

subClient.on('subscribe', (channel, count) => {
    logger.info(`Subscribe channel: ${channel}, Current total number of subscriptions: ${count}`);
});

subClient.on('message', (channel, message) => {
    try{
        message = JSON.parse(message);
        // 基于message的数据结构，当前对应publish(channel, JSON.stringify({'message': data}));
        let msg = message.message;

        msgCount ++;
        if (!Number.isSafeInteger(msgCount)) {
            msgCount = 0;
            logger.info('Message count is reset to 0');
        }

        switch (status) {
            case 0:
                if (regCollectStart.test(msg) || msgBuf) {
                    msgBuf += msg.trim();
                }
                if (!regCollectEnd.test(msg)) {
                    break;
                } else {
                    status = statusSet.filter;
                }
            case 1:
                if (filterMsgAllow(msgBuf)) {
                    status = statusSet.check;
                } else {
                    msgBuf = '';
                    status = statusSet.collect;
                    break;
                }
            case 2:
                if (checkMsgAllow(msgBuf)) {
                    status = statusSet.parser;
                } else {
                    logger.debug('checkMsgAllow: false, msgBuf:', msgBuf);
                    msgBuf = '';
                    status = statusSet.collect;
                    break;
                }
            case 3:
                const meta = parseMsg(msgBuf);
                const metaStr = JSON.stringify(meta);
                const timestamp = new Date().toISOString();
                if (regOrgnlMsgIdFieldParse.test(msgBuf) || regNtfctnFieldParse.test(msgBuf)) {
                    // 响应类报文，添加到已有的Hash Key中
                    const relationMsgId = (msgBuf.match(regOrgnlMsgIdFieldParse) || msgBuf.match(regNtfctnFieldParse))[1].trim();
                    const transactionKey = cmdOptions.transKeyPrefix + relationMsgId;
                    otherClient.exists(transactionKey, (err, res) => {
                        if (!err && res) {
                            otherClient.hset(transactionKey, timestamp, metaStr);
                        }
                    });
                } else {
                    // 请求类报文，创建两类Hash Key并分别设置过期时间
                    const msgId = meta.msgId;
                    const transactionKey = cmdOptions.transKeyPrefix + msgId;
                    const noticeKey = cmdOptions.noticeKeyPrefix + msgId;
                    otherClient.hmset(transactionKey, 'meta', metaStr, timestamp, metaStr, (err, res) => {
                        if (!err && res === 'OK') {
                            otherClient.EXPIRE(transactionKey, 300);
                        }
                    });
                    
                    otherClient.hset(noticeKey, msgId, metaStr, (err, res) => {
                        if (!err && res) {
                            otherClient.EXPIRE(noticeKey, 60);
                        }
                    });
                }
                msgBuf = '';
                status = statusSet.collect;
        }
    } catch (err) {
        logger.error('Parse error:', err);
    }
});

subClient.subscribe(cmdOptions.redisSub);

// 循环事件
let msgCountPrint = setInterval(() => {
    logger.info('Received log row total:', msgCount);
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
// node waiter.js -rsub pmts-01
