'use strict';

const sigtermHandler = require('sigterm-handler');
const _ = require('lodash');
const { kafka } = require('kafka-tools');
const signale = require('signale');
const { fork } = require('child_process');
const debug = require('debug')('break-rdkafka:command');

const genId = require('./generate-id');

const {
    KAFKA_BROKERS = 'localhost:9092,localhost:9093',
    NUM_CONSUMERS = '5',
    NUM_PRODUCERS = '3',
    NUM_PARTITIONS = '20',
    MESSAGES_PER_PARTITION = '200000',
    DEBUG = 'break-rdkafka',
    DISABLE_PAUSE_AND_RESUME = 'false',
    START_TIMEOUT = '1000',
    USE_COMMIT_ASYNC = 'false',
    BATCH_SIZE = '5000'
} = process.env;

const kafkaBrokers = KAFKA_BROKERS;
const numProducers = _.toSafeInteger(NUM_PRODUCERS);
const numConsumers = _.toSafeInteger(NUM_CONSUMERS);
const numPartitions = _.toSafeInteger(NUM_PARTITIONS);
const messagesPerPartition = _.toSafeInteger(MESSAGES_PER_PARTITION);
const startTimeout = _.toSafeInteger(START_TIMEOUT);
const batchSize = _.toSafeInteger(BATCH_SIZE);

function run() {
    const topicName = genId('break-kafka');

    debug('initializing...', {
        topicName,
        numPartitions,
        messagesPerPartition,
        numConsumers,
        numProducers,
        batchSize,
        startTimeout,
        DISABLE_PAUSE_AND_RESUME,
        USE_COMMIT_ASYNC,
        BATCH_SIZE
    });

    const messages = makeMessages();
    const totalMessages = _.size(messages);

    const client = new kafka.Client('localhost:2181', genId('break-kafka-'), {
        sessionTimeout: 5000,
        spinDelay: 500,
        retries: 0
    });

    client.zk.createTopic(topicName, numPartitions, 1, {}, (createTopicErr) => {
        if (createTopicErr) {
            signale.fatal(createTopicErr);
            return;
        }
        const children = {};
        const assignments = {};
        const produced = {};
        const consumed = {};

        _.times(numPartitions, (par) => {
            consumed[`${par}`] = 0;
            produced[`${par}`] = 0;
        });

        const exitNow = _.once(exit);
        let lastConsumedCount = -1;
        let lastProducedCount = -1;
        let deadTimeout = null;

        const updateInterval = setInterval(() => {
            const consumeCount = _.sum(_.values(consumed));
            const producedCount = _.sum(_.values(produced));
            const childrenCount = _.size(_.values(children));

            if (producedCount <= 0) {
                debug('UPDATE: waiting for messages to be produced...');
            } else if (producedCount > totalMessages) {
                debug(`UPDATE: all ${producedCount} messages produced`);
            } else if (lastProducedCount > 0 && producedCount !== lastProducedCount) {
                debug(`UPDATE: produced ${producedCount - lastProducedCount} more messages`);
            } else {
                debug('UPDATE: producing messages...', JSON.stringify(produced));
            }

            if (consumeCount <= 0) {
                debug('UPDATE: waiting for messages to be consumed...');
            } else if (consumeCount > totalMessages) {
                debug(`UPDATE: all ${consumeCount} messages consumed`);
            } else if (lastConsumedCount > 0 && consumeCount !== lastConsumedCount) {
                debug(`UPDATE: consumed ${producedCount - lastProducedCount} more messages`);
            } else {
                debug('UPDATE: consuming messages...', JSON.stringify(consumed));
            }

            debug(`UPDATE: active child processes ${childrenCount}`);

            if (consumeCount > 0 && consumeCount === lastConsumedCount) {
                if (deadTimeout != null) return;
                debug(`WARNING: Consume count (${consumeCount}) has stayed the same, will exit in 30 seconds if it doesn't changed`);
                deadTimeout = setTimeout(() => {
                    exitNow(new Error(`Consume count (${lastConsumedCount}) has stayed the same for 30 seconds`));
                }, 15 * 1000);
                return;
            }

            clearTimeout(deadTimeout);
            deadTimeout = null;
            lastConsumedCount = consumeCount;
            lastProducedCount = producedCount;
        }, 5000);

        function exit(err, done) {
            debug('Exiting in 5 seconds', err);
            clearInterval(updateInterval);
            clearTimeout(deadTimeout);

            _.delay(() => {
                debug('Exiting now...');

                killAll(() => {
                    client.zk.deleteTopics([topicName], (dErr) => {
                        if (dErr) {
                            signale.error(dErr);
                        }

                        if (err) {
                            signale.fatal(err);
                        }

                        signale.success('DONE!');
                        debug('assignments', assignments);
                        debug('produced', produced);
                        debug('consumed', consumed);
                        if (_.isFunction(done)) {
                            done();
                            return;
                        }

                        if (err) {
                            process.exit(1);
                        } else {
                            process.exit(0);
                        }
                    });
                });
            }, 5000);
        }

        function killAll(callback) {
            if (_.isEmpty(children)) {
                callback();
                return;
            }

            signale.warn('Killing all remaining children');

            _.forEach(_.values(children), (child) => {
                child.kill('SIGTERM');
            });

            setTimeout(() => {
                callback();
            }, 5 * 1000).unref();
        }

        function exitIfNeeded() {
            if (_.sum(_.values(produced)) > totalMessages) {
                exitNow();
            }
            if (_.isEmpty(children)) {
                exitNow();
            }
        }

        const shouldFinishChildren = _.once(() => {
            _.forEach(children, (child, key) => {
                debug(`sending shouldFinish to ${key}`);
                child.send({ fn: 'shouldFinish' });
            });
        });

        const debugToManyMessages = _.throttle((count) => {
            debug(`consumed more messages than it should have (${count})`);
        }, 1000);

        function consumedMessages(input) {
            _.forEach(input, (message) => {
                consumed[`${message.partition}`] += 1;
            });
            const count = _.sum(_.values(consumed));
            if (count === totalMessages) {
                signale.success(`consumed all of ${totalMessages} messages`);
                debug('consumed result', consumed);
                shouldFinishChildren();
            }
            if (count > totalMessages) {
                debugToManyMessages(count);
            }
        }

        function producedMessages(input) {
            _.forEach(input, (message) => {
                produced[`${message.partition}`] += 1;
            });
            const count = _.sum(_.values(produced));
            if (count === totalMessages) {
                signale.success(`produced all of ${totalMessages} messages`);
                debug('produced result', produced);
            }
            if (count > totalMessages) {
                debug(`produced more messages than it should have (${count})`);
            }
        }

        _.times(numProducers, (i) => {
            const key = `produce-${i}`;
            signale.time(key);

            const child = fork(`${__dirname}/produce-worker.js`, [], {
                env: {
                    BREAK_KAFKA_KEY: key,
                    BREAK_KAFKA_TOPIC_NAME: topicName,
                    BREAK_KAFKA_BROKERS: kafkaBrokers,
                    BATCH_SIZE: batchSize,
                    FORCE_COLOR: '1',
                    DEBUG,
                },
                stdio: 'inherit'
            });

            child.on('close', (code) => {
                delete children[key];
                if (code !== 0) {
                    exitNow(new Error(`${key} died with an exit code of ${code}`));
                    return;
                }

                exitIfNeeded();
                signale.timeEnd(key);
                signale.success(`${key} done!`);
            });

            child.on('error', (err) => {
                signale.error(err);
            });

            child.on('message', (data) => {
                if (data.fn === 'producedMessages') {
                    producedMessages(data.msg);
                }
                if (data.fn === 'getMessageBatch') {
                    const randomStart = _.random(0, _.size(messages));
                    const batch = messages.splice(randomStart, 5000);
                    const responseId = data.requestId;
                    child.send({ fn: 'receiveMessageBatch', messages: batch, responseId });
                }
            });

            children[key] = child;
        });

        _.times(numConsumers, (i) => {
            const key = `consume-${i + 1}`;

            signale.time(key);

            const child = fork(`${__dirname}/consume-worker.js`, [], {
                env: {
                    BREAK_KAFKA_KEY: key,
                    BREAK_KAFKA_TOPIC_NAME: topicName,
                    BREAK_KAFKA_BROKERS: kafkaBrokers,
                    BATCH_SIZE: batchSize,
                    FORCE_COLOR: '1',
                    START_TIMEOUT: i * startTimeout,
                    DEBUG,
                    DISABLE_PAUSE_AND_RESUME,
                    USE_COMMIT_ASYNC
                },
                stdio: 'inherit'
            });

            child.on('close', (code) => {
                delete children[key];

                if (code > 0) {
                    exitNow(new Error(`${key} died with an exit code of ${code}`));
                    return;
                }

                exitIfNeeded();
                signale.timeEnd(key);
                signale.success(`${key} done!`);
            });

            child.on('error', (err) => {
                signale.error(err);
            });

            child.on('message', (data) => {
                if (data.fn === 'consumedMessages') {
                    consumedMessages(data.msg);
                }
                if (data.fn === 'updateAssignments') {
                    assignments[key] = data.assignments;
                }
            });

            children[key] = child;
        });

        sigtermHandler(() => new Promise((resolve, reject) => {
            exit(null, (err) => {
                if (err) {
                    reject(err);
                } else {
                    setTimeout(() => {
                        resolve();
                    }, 5000);
                }
            });
        }));
    });

    function makeMessages() {
        signale.time('making messages');
        const results = [];
        _.times(numPartitions, (partition) => {
            _.times(messagesPerPartition, () => {
                const key = genId(null, 15);
                results.push({ key, partition });
            });
        });
        signale.timeEnd('making messages');
        return results;
    }
}

run();
