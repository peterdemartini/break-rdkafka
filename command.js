'use strict';

const _ = require('lodash');
const { kafka } = require('kafka-tools');
const uuidv4 = require('uuid/v4');
const signale = require('signale');
const { fork } = require('child_process');

const {
    KAFKA_BROKERS = 'localhost:9092,localhost:9093',
    NUM_CONSUMERS = '5',
    NUM_PRODUCERS = '2',
    NUM_PARTITIONS = '25',
    MESSAGES_PER_PARTITION = '100000'
} = process.env;

const kafkaBrokers = KAFKA_BROKERS;
const numProducers = _.toSafeInteger(NUM_PRODUCERS);
const numConsumers = _.toSafeInteger(NUM_CONSUMERS);
const numPartitions = _.toSafeInteger(NUM_PARTITIONS);
const messagesPerPartition = _.toSafeInteger(MESSAGES_PER_PARTITION);

function run() {
    const topicName = uuidv4();

    const messages = makeMessages(messagesPerPartition, numPartitions);

    const totalMessages = _.size(messages);
    const client = new kafka.Client('localhost:2181', _.uniqueId('break-kafka-'), {
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
        const finalOffsets = {};
        const processed = {};
        const sent = {};

        _.times(numPartitions, (par) => {
            sent[`${par}`] = 0;
            processed[`${par}`] = 0;
        });

        const updateInterval = setInterval(() => {
            signale.info(`UPDATE: processed: ${_.sum(_.values(processed))} sent: ${_.sum(_.values(sent))}`);
        }, 1000);

        const exitNow = _.once((err) => {
            signale.info('Exiting in 5 seconds');
            clearInterval(updateInterval);

            _.delay(() => {
                signale.info('Exiting now...');

                killAll(() => {
                    client.zk.deleteTopics([topicName], (dErr) => {
                        if (dErr) {
                            signale.error(dErr);
                        }

                        if (err) {
                            signale.fatal(err);
                            process.exit(1);
                        }

                        signale.success('DONE!');
                        signale.log('offsets', finalOffsets);
                        signale.log('processed', processed);
                        signale.log('sent', sent);
                        process.exit(0);
                    });
                });
            }, 5000);
        });

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
            if (_.sum(_.values(processed)) > totalMessages) {
                exitNow();
            }
            if (_.isEmpty(children)) {
                exitNow();
            }
        }

        const shouldFinishChildren = _.once(() => {
            _.forEach(children, (child, key) => {
                signale.info(`sending shouldFinish to ${key}`);
                child.send({ fn: 'shouldFinish' });
            });
        });

        const sentMessage = (input) => {
            sent[`${input.partition}`] += 1;
            if (_.sum(_.values(sent)) === totalMessages) {
                signale.warn('sent all of the messages');
            }
            if (_.sum(_.values(sent)) >= totalMessages) {
                signale.warn('sent more messages than it should have');
            }
        };

        const logProcessedMessage = _.throttle((input) => {
            signale.warn('processed more messages than it should have', JSON.stringify(input, null, 2));
        }, 100);

        const processedMessage = (input) => {
            processed[`${input.partition}`] += 1;
            if (_.sum(_.values(processed)) === totalMessages) {
                signale.info('processed all of the messages');
                shouldFinishChildren();
            }
            if (_.sum(_.values(processed)) > totalMessages) {
                logProcessedMessage(input);
            }
        };

        _.times(numProducers, (i) => {
            const key = `produce-${i}`;
            signale.time(key);

            const child = fork(`${__dirname}/produce-worker.js`, [], {
                env: {
                    BREAK_KAFKA_KEY: key,
                    BREAK_KAFKA_TOPIC_NAME: topicName,
                    BREAK_KAFKA_BROKERS: kafkaBrokers,
                    FORCE_COLOR: '1'
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
                if (data.fn === 'sentMessage') {
                    sentMessage(data.msg);
                }
                if (data.fn === 'getMessageBatch') {
                    const batch = messages.splice(0, 1000);
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
                    FORCE_COLOR: '1',
                    START_TIMEOUT: i * 5000
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
                if (data.fn === 'processedMessage') {
                    processedMessage(data.msg);
                }
                if (data.fn === 'updateOffsets') {
                    finalOffsets[key] = data.offsets;
                }
                if (data.fn === 'ready') {
                    signale.info('worker ready');
                }
            });
            children[key] = child;
        });


        signale.info(`initializing...
            topic: ${topicName}
            numPartitions: ${numPartitions}
            totalMessages: ${totalMessages}
            consumers: ${numConsumers}
            producers: ${numProducers}
        `);
    });
}

function makeMessages(count, partitions) {
    const perPartition = () => _.times(partitions, partition => ({
        partition,
        key: uuidv4()
    }));
    return _.flatten(_.times(count, perPartition));
}

run();
