'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const exitHandler = require('./exit-handler');
const genId = require('./generate-id');

const useCommitAsync = process.env.USE_COMMIT_ASYNC === 'true';
const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const disablePauseAndResume = process.env.DISABLE_PAUSE_AND_RESUME === 'true';
const useConsumerPauseAndResume = process.env.USE_CONSUMER_PAUSE_AND_RESUME === 'true';
const startTimeout = parseInt(process.env.START_TIMEOUT, 10);
const batchSize = parseInt(process.env.BATCH_SIZE, 10);

function _consumer({
    updateAssignments,
    consumedMessages,
    reportError
}, callback) {
    let paused = false;
    let rebalancing = false;
    let randomTimeoutId;

    let assignments = [];
    let consumeTimeout;
    let processed = 0;
    const consumerDone = _.once(_consumerDone);

    if (!topicName) {
        console.error('requires a topicName'); // eslint-disable-line no-console
        process.exit(1);
    }

    function simpleTopics(input) {
        let topics = input;
        if (_.isPlainObject(input)) {
            topics = _.values(input);
        }
        return _.map(topics, ({ partition, offset = 'N/A' }) => {
            const message = `P: ${partition}, O: ${offset}`;
            return message;
        }).join(', ');
    }

    debug('initializing...');
    const finishInterval = setInterval(() => {
        updateAssignments(assignments);
    }, 500);

    const consumer = new Kafka.KafkaConsumer({
        'client.id': genId('break-kafka-'),
        debug: 'cgrp,topic',
        'metadata.broker.list': kafkaBrokers,
        'group.id': `${topicName}-group`,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        'auto.offset.reset': 'smallest',
        offset_commit_cb(err, topicPartitions) {
            if (err) {
                // There was an error committing
                reportError(err);
            } else {
                // Commit went through. Let's log the topic partitions
                debug('COMMIT DONE', simpleTopics(topicPartitions));
            }
        },
        rebalance_cb(err, changed) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                rebalancing = false;
                const updated = _.filter(changed, ({ partition }) => {
                    const isNew = !_.some(assignments, { partition });
                    return isNew;
                });
                assignments = _.unionBy(assignments, updated, 'partition');
                debug('REBALANCE: assigned', simpleTopics(assignments));

                this.assign(changed);
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                rebalancing = true;
                debug('REBALANCE: unassigned', simpleTopics(changed));

                this.unassign(changed);
            } else {
                // We had a real error
                reportError(err);
            }
        }
    });

    let ended = false;

    // logging debug messages, if debug is enabled
    consumer.on('event.log', (log) => {
        if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
            debug(log.message);
        }
    });

    // logging all errors
    consumer.on('event.error', (err) => {
        reportError(err);
    });

    consumer.on('ready', () => {
        debug('ready!');

        debug(`Waiting for ${startTimeout}ms before starting...`);
        consumer.subscribe([topicName]);

        setTimeout(() => {
            debug('Starting...');
            // start consuming messages
            consume();
            if (!disablePauseAndResume) randomlyPauseAndResume();
        }, startTimeout);
    });

    function consume() {
        clearTimeout(consumeTimeout);
        if (ended) {
            return;
        }
        consumeTimeout = setTimeout(() => {
            if (paused || rebalancing) {
                consume();
                return;
            }
            consumer.setDefaultConsumeTimeout(batchSize);
            consumer.consume(batchSize, (err, messages) => {
                if (err) {
                    reportError(err);
                    consume();
                    return;
                }

                if (_.isEmpty(messages)) {
                    consume();
                    return;
                }

                const offsets = {};
                _.forEach(messages, (message) => {
                    const { offset, partition, topic } = message;
                    assignments = _.map(assignments, (assignment) => {
                        if (assignment.partition === partition) {
                            assignment.offset = offset;
                        }
                        return assignment;
                    });
                    const current = _.get(offsets, [partition, 'offset'], 0);
                    if (offset > current) {
                        _.set(offsets, partition, { offset, partition, topic });
                    }
                    processed += 1;
                });

                if (paused || rebalancing) {
                    debug('WARNING: about to commit when paused or rebalancing');
                }

                if (!useCommitAsync) {
                    debug('committing sync...', simpleTopics(offsets));
                }

                _.forEach(_.values(offsets), (message) => {
                    if (useCommitAsync) {
                        consumer.commit(message);
                    } else {
                        consumer.commitSync(message);
                    }
                });
                if (!useCommitAsync) {
                    debug('committing took');
                }
                consumedMessages(messages);
                consume();
            });
        }, 100);
    }


    consumer.on('disconnected', () => {
        debug('WARNING: consumer disconnected');
        consumerDone(new Error('Consumer Disconnected'));
    });

    // starting the consumer
    consumer.connect({}, (err) => {
        if (err) {
            reportError(err);
            return;
        }
        debug('connected');
    });

    function _consumerDone(err, cb = callback) {
        debug('done!');
        clearTimeout(consumeTimeout);
        clearTimeout(randomTimeoutId);
        clearInterval(finishInterval);
        ended = true;
        debug('assignments', simpleTopics(assignments));
        if (consumer.isConnected()) {
            consumer.disconnect();
        }
        if (err) {
            console.error(err); // eslint-disable-line no-console
            cb(err);
            return;
        }
        debug(`processed ${processed}`);
        callback(null);
    }

    function randomlyPauseAndResume() {
        if (_.isEmpty(assignments)) {
            randomTimeoutId = setTimeout(() => {
                randomlyPauseAndResume();
            }, 1000);
            return;
        }

        if (_.random(0, 5)) {
            randomTimeoutId = setTimeout(() => {
                randomlyPauseAndResume();
            }, 5000);
            return;
        }

        const pauseTimeout = _.random(1000, 30 * 1000);
        const resumeTimeout = _.random(1000, 15 * 1000);
        debug(`CHOAS: will pause in ${pauseTimeout}ms and resume in ${resumeTimeout}ms`);

        randomTimeoutId = setTimeout(() => {
            pause();

            randomTimeoutId = setTimeout(() => {
                resume();
                randomlyPauseAndResume();
            }, resumeTimeout);
        }, pauseTimeout);
    }

    function pause() {
        if (disablePauseAndResume) return;
        if (paused) return;
        debug('PAUSING!');
        if (useConsumerPauseAndResume) {
            consumer.pause(assignments);
        }
        paused = true;
    }

    function resume() {
        if (disablePauseAndResume) return;
        if (!paused) return;
        debug('RESUMING!');
        if (useConsumerPauseAndResume) {
            consumer.resume(assignments);
        }
        paused = false;
    }

    exitHandler(() => new Promise((resolve, reject) => {
        _consumerDone(null, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    }));
}

module.exports = _consumer;
