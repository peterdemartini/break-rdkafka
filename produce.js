'use strict';

const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const Kafka = require('node-rdkafka');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const exitHandler = require('./exit-handler');
const genId = require('./generate-id');

const messageObject = fs.readFileSync('message.json');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const batchSize = parseInt(process.env.BATCH_SIZE, 10);

const statsFile = path.join(__dirname, 'stats', `${process.env.BREAK_KAFKA_KEY}.json`);
try {
    fs.unlinkSync(statsFile);
} catch (err) {
    // this is okay
}

function produce({ producedMessages, startBatch, reportError }, callback) {
    debug('initializing...');

    let ended = false;
    let processing = false;

    const producer = new Kafka.Producer({
        'client.id': genId('break-kafka-'),
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
        if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
            debug(log.message);
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
                    producer.flush(60000, (flusherr) => {
                        if (flusherr) reportError(flusherr);
                        producedMessages(messages);
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

    function producerDone(err, cb = callback) {
        if (ended && err) {
            reportError(err);
            return;
        }
        let timeout;
        const done = () => {
            debug('done!');
            if (err) {
                console.error(err); // eslint-disable-line no-console
                cb(err);
                return;
            }
            cb();
        };
        ended = true;

        function checkFinished() {
            clearTimeout(timeout);
            timeout = setTimeout(() => {
                if (producer.isConnected()) {
                    checkFinished();
                    return;
                }
                if (processing) {
                    checkFinished();
                    return;
                }
                done();
            }, 100);
        }

        producer.flush(60000, (flusherr) => {
            if (flusherr) console.error(flusherr); // eslint-disable-line no-console
            checkFinished();
        });
    }

    exitHandler(() => new Promise((resolve, reject) => {
        producerDone(null, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    }));
}

module.exports = produce;
