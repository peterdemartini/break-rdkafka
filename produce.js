'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const exitHandler = require('./exit-handler');
const genId = require('./generate-id');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const batchSize = parseInt(process.env.BATCH_SIZE, 10);

if (!topicName) {
    console.error('requires a topicName'); // eslint-disable-line no-console
    process.exit(1);
}

function produce({ producedMessages, startBatch }, callback) {
    debug('initializing...');

    let ended = false;
    const producerDone = _.once(_producerDone);

    const producer = new Kafka.Producer({
        'client.id': genId('break-kafka-'),
        'compression.codec': 'gzip',
        debug: 'broker,topic',
        'queue.buffering.max.messages': batchSize * 5,
        'queue.buffering.max.ms': 10 * 1000,
        'topic.metadata.refresh.interval.ms': 10000,
        'batch.num.messages': batchSize,
        'metadata.broker.list': kafkaBrokers,
        'log.connection.close': false
    });

    // logging debug messages, if debug is enabled
    producer.on('event.log', (log) => {
        if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
            debug(log.message);
        }
    });

    producer.setPollInterval(100);

    // logging all errors
    producer.on('event.error', (err) => {
        console.error(err); // eslint-disable-line no-console
    });

    // Wait for the ready event before producing
    producer.on('ready', () => {
        debug('ready!');
        const processBatch = () => {
            startBatch((err, messages) => {
                if (err) {
                    producerDone(err);
                    return;
                }
                if (_.isEmpty(messages)) {
                    producerDone();
                    return;
                }
                const count = _.size(messages);
                const sendAfter = _.after(count, () => {
                    producer.flush(60000, (flusherr) => {
                        if (err) debug('flush error', flusherr);
                        producedMessages(messages);
                        setImmediate(() => {
                            processBatch();
                        });
                    });
                });
                _.forEach(messages, (message) => {
                    if (ended) {
                        return;
                    }
                    const value = Buffer.from(message.key);
                    const result = producer.produce(
                        topicName,
                        message.partition,
                        value,
                        null,
                        Date.now()
                    );
                    if (result !== true) {
                        debug(`produce did not return true, got ${result}`);
                    }
                    sendAfter();
                });
            });
        };
        processBatch();
    });

    producer.on('disconnected', () => {
        producerDone(new Error('Producer Disconnected'));
    });

    // starting the producer
    producer.connect();

    function _producerDone(err, cb = callback) {
        debug('done!');
        ended = true;
        if (producer.isConnected()) {
            producer.disconnect();
        }
        if (err) {
            console.error(err); // eslint-disable-line no-console
            cb(err);
            return;
        }
        cb();
    }

    exitHandler(signal => new Promise((resolve, reject) => {
        debug(`caught ${signal} handler`);
        _producerDone(null, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    }));
}

module.exports = produce;
