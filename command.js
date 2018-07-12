'use strict';

const _ = require('lodash');
const { kafka } = require('kafka-tools');
const signale = require('signale');
const { fork } = require('child_process');
const stringifyObject = require('stringify-object');
const debug = require('debug')('break-rdkafka:command');
const exitHandler = require('./exit-handler');

const genId = require('./generate-id');

const breakKafka = process.env.BREAK_KAFKA === 'true';

const {
    KAFKA_BROKERS = 'localhost:9092,localhost:9093',
    NUM_CONSUMERS = '5',
    NUM_PRODUCERS = '3',
    NUM_PARTITIONS = '15',
    MESSAGES_PER_PARTITION = '30000',
    DEBUG = 'break-rdkafka',
    DISABLE_PAUSE_AND_RESUME = breakKafka ? 'false' : 'true',
    USE_CONSUMER_PAUSE_AND_RESUME = breakKafka ? 'false' : 'true',
    START_TIMEOUT = breakKafka ? '5000' : '1000',
    USE_COMMIT_ASYNC = breakKafka ? 'false' : 'true',
    BATCH_SIZE = '10000',
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

    if (breakKafka) {
        debug('WARNING: CONFIGURED TO BREAK KAFKA');
    }

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
        USE_CONSUMER_PAUSE_AND_RESUME,
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

        let lastConsumedCount = -1;
        let lastProducedCount = -1;
        let consumeStuckCount = 0;
        let produceStuckCount = 0;
        let exitTimeout = null;
        const reportedErrors = [];

        function getConsumedCount() {
            return _.sum(_.values(consumed));
        }

        function getProducedCount() {
            return _.sum(_.values(produced));
        }

        const updateInterval = setInterval(() => {
            const consumedCount = getConsumedCount();
            const producedCount = getProducedCount();
            const childrenCount = _.size(_.values(children));
            const updates = [];

            if (producedCount <= 0) {
                updates.push('waiting for messages to be produced...');
            } else if (producedCount >= totalMessages) {
                updates.push(`all ${producedCount} messages have been produced`);
            } else {
                const remaining = totalMessages - producedCount;
                updates.push(`produced ${producedCount - lastProducedCount}, ${remaining} remaining...`);
            }

            if (consumedCount <= 0) {
                updates.push('waiting for messages to be consumed...');
            } else if (consumedCount >= totalMessages) {
                updates.push(`all ${consumedCount} messages have been consumed`);
            } else {
                const remaining = totalMessages - consumedCount;
                updates.push(`consumed ${consumedCount - lastConsumedCount}, ${remaining} remaining...`);
            }

            updates.push(`active child processes ${childrenCount}`);

            if (consumedCount > 0 && consumedCount < totalMessages
                && consumedCount && consumedCount === lastConsumedCount) {
                reportedErrors.push(`WARNING: consumed count ${producedCount} stayed the same`);
                consumeStuckCount += 1;
            } else {
                consumeStuckCount = 0;
            }

            if (producedCount > 0 && producedCount < totalMessages
                && producedCount === lastProducedCount) {
                reportedErrors.push(`WARNING: produced count ${producedCount} stayed the same`);
                produceStuckCount += 1;
            } else {
                produceStuckCount = 0;
            }

            const errCount = _.size(reportedErrors);
            if (!errCount) {
                updates.push('no reported errors');
            } else {
                updates.push(...reportedErrors);
            }


            debug(`UPDATES:\n - ${updates.join('\n - ')}\n`);

            if (consumeStuckCount === 3) {
                exit(new Error(`Consume count (${consumedCount}) has stayed the same for 30 seconds`));
            }

            if (produceStuckCount === 3) {
                exit(new Error(`Produce count (${producedCount}) has stayed the same for 30 seconds`));
            }

            lastConsumedCount = consumedCount;
            lastProducedCount = producedCount;
        }, 10 * 1000);

        function exit(err, signal, done) {
            if (exitTimeout != null) {
                if (err) {
                    reportedErrors.push(err.stack ? err.stack : err.toString());
                }
                debug('already exiting...');
                return;
            }

            if (err) {
                signale.error('Will exit because to error: ', err);
            }

            clearInterval(updateInterval);
            debug('Exiting in 5 seconds...');

            exitTimeout = setTimeout(() => {
                debug('Exiting now...');
                killAll(signal, (killAllErr) => {
                    if (killAllErr) signale.error(killAllErr);

                    client.zk.deleteTopics([topicName], (dErr) => {
                        if (dErr) {
                            signale.error(dErr);
                        }
                        debug(`DELETED TOPIC: ${topicName}`);
                        debug('assignments', stringifyObject(assignments));
                        debug('produced', stringifyObject(produced));
                        debug('consumed', stringifyObject(consumed));
                        debug('reportedErrors', stringifyObject(reportedErrors));

                        if (err) {
                            signale.fatal(err);
                            if (_.isFunction(done)) {
                                done(err);
                                return;
                            }
                            process.exit(1);
                        }

                        signale.success('DONE!');
                        if (_.isFunction(done)) {
                            done();
                            return;
                        }
                        process.exit(0);
                    });
                });
            }, 5000);
        }

        function killAll(signal = 'SIGTERM', callback) {
            if (_.isEmpty(children)) {
                callback();
                return;
            }
            let waitUntilTimeout;
            let shutdownTimeout;
            signale.warn(`${signal} all remaining children`);

            _.forEach(_.values(children), (child) => {
                child.kill(signal);
            });

            function waitUntilDead() {
                debug(`waiting until child processes are dead... ${_.size(_.values(children))}`);
                waitUntilTimeout = setTimeout(() => {
                    if (_.isEmpty(children)) {
                        clearTimeout(shutdownTimeout);
                        callback();
                        return;
                    }
                    waitUntilDead();
                }, 1000);
            }
            waitUntilDead();

            shutdownTimeout = setTimeout(() => {
                clearTimeout(waitUntilTimeout);
                callback(new Error('Failed to shutdown children in 10 seconds'));
            }, 10 * 1000);
        }

        function exitIfNeeded() {
            if (getProducedCount() > totalMessages) {
                exit();
            }
            if (_.isEmpty(children)) {
                exit();
            }
        }

        const debugToManyMessages = _.debounce((count) => {
            debug(`consumed more messages than it should have (${count})`);
        }, 1000);

        function consumedMessages(input) {
            _.forEach(input, (message) => {
                consumed[`${message.partition}`] += 1;
            });
            const count = getConsumedCount();
            if (count === totalMessages) {
                signale.success(`consumed all of ${totalMessages} messages`);
                debug('consumed result', consumed);
                exit();
            }
            if (count > totalMessages) {
                debugToManyMessages(count);
            }
        }

        function producedMessages(input) {
            _.forEach(input, (message) => {
                produced[`${message.partition}`] += 1;
            });
            const count = getProducedCount();
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
            let heartbeatTimeout;
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
                clearTimeout(heartbeatTimeout);

                if (exitTimeout == null && !_.isEmpty(messages)) {
                    const message = `WARNING: child ${key} exited with status code ${code}`;
                    debug(message);
                    reportedErrors.push(message);
                }

                if (code !== 0) {
                    exit(new Error(`${key} died with an exit code of ${code}`));
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
                    const remaining = _.size(messages);
                    if (!remaining) {
                        debug(`no more messages to use, sending SIGTERM to ${key}`);
                        child.kill('SIGTERM');
                        return;
                    }
                    const count = remaining > batchSize ? batchSize : remaining;
                    const randomStart = _.random(0, remaining);
                    const batch = messages.splice(randomStart, count);
                    const responseId = data.requestId;
                    child.send({ fn: 'receiveMessageBatch', messages: batch, responseId });
                }

                if (data.fn === 'heartbeat') {
                    clearTimeout(heartbeatTimeout);
                    heartbeatTimeout = setTimeout(() => {
                        if (exitTimeout == null) {
                            const message = `WARNING: ${key} hasn't responded to heartbeats in ${data.validFor}ms sending SIGKILL`;
                            debug(message);
                            reportedErrors.push(message);
                        }
                        heartbeatTimeout = setTimeout(() => {
                            child.kill('SIGKILL');
                        }, data.validFor);
                    }, data.validFor);
                }

                if (data.fn === 'reportError') {
                    if (exitTimeout == null) {
                        const message = `ERROR: ${key} reported error ${data.error}`;
                        debug(message);
                        reportedErrors.push(message);
                    }
                }
            });

            children[key] = child;
        });

        _.times(numConsumers, (i) => {
            const key = `consume-${i + 1}`;
            let heartbeatTimeout;
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
                clearTimeout(heartbeatTimeout);

                if (exitTimeout == null) {
                    const message = `WARNING: child ${key} exited with status code ${code}`;
                    debug(message);
                    reportedErrors.push(message);
                }

                if (code > 0) {
                    exit(new Error(`${key} died with an exit code of ${code}`));
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
                if (data.fn === 'heartbeat') {
                    clearTimeout(heartbeatTimeout);
                    heartbeatTimeout = setTimeout(() => {
                        if (exitTimeout == null) {
                            const message = `WARNING: ${key} hasn't responded to heartbeats in ${data.validFor}ms sending SIGKILL`;
                            debug(message);
                            reportedErrors.push(message);
                        }
                        child.kill('SIGKILL');
                    }, data.validFor);
                }

                if (data.fn === 'reportError') {
                    if (exitTimeout == null) {
                        const message = `ERROR: ${key} reported error ${data.error}`;
                        debug(message);
                        reportedErrors.push(message);
                    }
                }
            });

            children[key] = child;
        });

        exitHandler(signal => new Promise((resolve, reject) => {
            exit(null, signal, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        }));
    });

    function makeMessages() {
        signale.time('making messages');
        const results = [];
        _.times(numPartitions, (partition) => {
            const partionMessages = _.times(messagesPerPartition, () => ({ partition }));
            results.push(...partionMessages);
        });
        signale.timeEnd('making messages');
        return results;
    }
}

run();
