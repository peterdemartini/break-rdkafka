'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const genId = require('./generate-id');

function _consumer(options, callback) {
    const {
        topicName,
        updateAssignments,
        shouldFinish,
        processedMessage,
        kafkaBrokers,
        startTimeout
    } = options;

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

    debug('initializing...');
    const finishInterval = setInterval(() => {
        updateAssignments(assignments);
        if (shouldFinish()) {
            debug('consumer was told it should finish');
            consumerDone();
        }
    }, 500);


    const consumer = new Kafka.KafkaConsumer({
        'client.id': genId('break-kafka-'),
        debug: 'cgrp,topic',
        'metadata.broker.list': kafkaBrokers,
        'group.id': `${topicName}-group`,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        'auto.offset.reset': 'beginning',
        rebalance_cb(err, changed) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                debug('REBALANCE: assigned', _.map(changed, 'partition'));

                assignments = _.filter(assignments, ({ partition }) => _.some(changed, { partition }));

                this.assign(changed);
                rebalancing = false;
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                debug('REBALANCE: unassigned', _.map(changed, 'partition'));

                this.pause(changed);
                this.unassign(changed);
                rebalancing = true;
            } else {
                // We had a real error
                console.error(err); // eslint-disable-line no-console
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
        console.error(err); // eslint-disable-line no-console
    });

    const numMessages = 1000;

    consumer.on('ready', () => {
        debug('ready!');

        debug(`Waiting for ${startTimeout}ms before starting...`);
        consumer.subscribe([topicName]);

        setTimeout(() => {
            debug('Starting...');
            // start consuming messages
            consume();
        }, startTimeout);

        randomlyPauseAndResume();
    });

    function consume() {
        clearTimeout(consumeTimeout);
        if (ended) {
            return;
        }
        consumeTimeout = setTimeout(() => {
            if (paused || rebalancing) {
                debug('currently paused or rebalancing will try consuming soon');
                consume();
                return;
            }
            consumer.setDefaultConsumeTimeout(1000);
            consumer.consume(numMessages, (err, messages) => {
                if (err) {
                    debug('consume error', err);
                    consumerDone(err);
                    return;
                }
                if (_.isEmpty(messages)) {
                    consume();
                    return;
                }

                const offsets = {};
                _.forEach(messages, (message) => {
                    const { offset, partition } = message;
                    assignments = _.map(assignments, (assignment) => {
                        if (assignment.partition === partition) {
                            assignment.offset = offset;
                        }
                        return assignment;
                    });
                    const current = _.get(offsets, [partition, 'offset'], 0);
                    if (offset > current) {
                        _.set(offsets, partition, message);
                    }
                    processedMessage({ partition, offset });
                    processed += 1;
                });

                if (paused || rebalancing) {
                    debug('WARNING: about to commit when paused or rebalancing');
                }

                _.forEach(_.values(offsets), (message) => {
                    consumer.commit(message);
                });

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
            console.error(err); // eslint-disable-line no-console
            return;
        }
        debug('connected');
    });

    function _consumerDone(err) {
        debug('done!');
        clearTimeout(consumeTimeout);
        clearTimeout(randomTimeoutId);
        clearInterval(finishInterval);
        ended = true;
        debug('assignments', assignments);
        if (consumer.isConnected()) {
            consumer.disconnect();
        }
        if (err) {
            console.error(err); // eslint-disable-line no-console
            callback(err);
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

        if (!_.random(0, 10)) {
            randomTimeoutId = setTimeout(() => {
                randomlyPauseAndResume();
            }, 5000);
            return;
        }

        const pauseTimeout = _.random(1000, 30000);
        const resumeTimeout = _.random(1000, 30000);
        debug(`CHAOS: will pause in ${pauseTimeout}ms and resume in ${resumeTimeout}ms`);

        randomTimeoutId = setTimeout(() => {
            if (!paused) {
                // consumer.pause(assignments);
                paused = true;
            }

            randomTimeoutId = setTimeout(() => {
                if (paused) {
                    // consumer.resume(assignments);
                    paused = false;
                }
                randomlyPauseAndResume();
            }, resumeTimeout);
        }, pauseTimeout);
    }
}

module.exports = _consumer;
