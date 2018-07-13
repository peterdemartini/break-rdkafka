'use strict';

const producerId = process.env.PRODUCER_ID;

const Promise = require('bluebird');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const Kafka = require('node-rdkafka');
const debug = require('debug')(`break-rdkafka:${producerId}`);
const genId = require('./generate-id');
const exitHandler = require('./exit-handler');

const messageObject = fs.readFileSync('message.json');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const batchSize = parseInt(process.env.BATCH_SIZE, 10);

let ended = false;
let processing = false;

const statsFile = path.join(__dirname, 'stats', `${producerId}.json`);
try {
    fs.unlinkSync(statsFile);
} catch (err) {
    // this is okay
}

const producedMessages = (msg) => {
    process.send({ fn: 'producedMessages', msg });
};

const startBatch = (callback) => {
    if (ended) {
        callback();
        return;
    }

    const requestId = genId('batch');
    process.send({ fn: 'getMessageBatch', requestId });

    const onMessage = ({ fn, messages, responseId }) => {
        if (fn !== 'receiveMessageBatch') {
            return;
        }
        if (requestId !== responseId) {
            return;
        }
        process.removeListener('message', onMessage);
        callback(null, messages);
    };
    process.on('message', onMessage);
};

const heartbeatInterval = setInterval(() => {
    if (ended) return;
    process.send({ fn: 'heartbeat', validFor: 10000 });
}, 2000).unref();

const reportError = (error) => {
    if (ended) return;
    process.send({ fn: 'reportError', error: error.stack ? error.stack : error.toString() });
};

debug('initializing...');

const producer = new Kafka.Producer({
    'client.id': producerId,
    debug: 'broker,topic,msg',
    'queue.buffering.max.messages': batchSize * 5,
    'queue.buffering.max.ms': 10 * 1000,
    'batch.num.messages': batchSize,
    'metadata.broker.list': kafkaBrokers,
    'log.connection.close': true,
    'statistics.interval.ms': 5000,
});

    // logging debug messages, if debug is enabled
producer.on('event.log', (log) => {
    if (/(fail|error|warn|issue|disconnect|problem|unable|invalid|rebalance)/gi.test(log.message)) {
        debug('DEBUG', log.message);
    }
});

producer.setPollInterval(100);

producer.on('event.error', (err) => {
    reportError(err);
});

producer.on('event.throttle', (err) => {
    reportError(err);
});

producer.on('event.stats', ({ message }) => {
    try {
        const stats = JSON.parse(message);
        stats.COLLECTED_AT = new Date().toString();
        fs.writeFileSync(statsFile, JSON.stringify(stats, null, 2));
    } catch (err) {
        reportError(err);
    }
});

// Wait for the ready event before producing
producer.on('ready', () => {
    debug('ready!');
    const processBatch = () => {
        if (ended) return;
        processing = true;
        startBatch((err, messages) => {
            if (err) {
                reportError(err);
                processBatch();
                return;
            }
            if (_.isEmpty(messages)) {
                debug('got empty messages, ending...');
                producerDone();
                return;
            }

            const count = _.size(messages);

            const sendAfter = _.after(count, () => {
                producedMessages(messages);

                if (!producer.isConnected()) return;

                producer.flush(60000, (flusherr) => {
                    if (flusherr) reportError(flusherr);
                    setImmediate(() => {
                        processing = false;
                        processBatch();
                    });
                });
            });

            _.forEach(messages, (message) => {
                if (ended) {
                    return;
                }

                try {
                    const result = producer.produce(
                        topicName,
                        message.partition,
                        messageObject,
                        null,
                        Date.now()
                    );
                    if (result !== true) {
                        reportError(new Error(`produce did not return true, got ${result}`));
                    }
                } catch (produceErr) {
                    reportError(produceErr);
                }

                sendAfter();
            });
        });
    };
    processBatch();
});

producer.on('disconnected', () => {
    if (ended) return;
    producerDone(new Error('Producer Disconnected'));
});

// starting the producer
producer.connect();

async function checkProcessing() {
    if (!processing) {
        return;
    }
    await Promise.delay(100);
    await checkProcessing();
}

async function checkDisconnect() {
    if (!producer.isConnected()) {
        return;
    }
    await Promise.delay(100);
    await checkDisconnect();
}

async function producerDone(err, skipExit) {
    if (ended && err) {
        console.error('GOT ERROR after shutdown', err); // eslint-disable-line no-console
        return;
    }

    ended = true;

    if (producer.isConnected()) {
        await checkProcessing();

        const flush = Promise.promisify(producer.flush, { context: err });

        if (producer.isConnected()) {
            await flush(60000);
        }
    }

    producer.disconnect();

    await checkDisconnect();
    clearInterval(heartbeatInterval);

    if (skipExit) return;

    if (err) {
        console.error(err); // eslint-disable-line no-console
        process.exit(1);
    }
    process.exit(0);
}

exitHandler((signal, err) => producerDone(err, true), 5 * 1000);
