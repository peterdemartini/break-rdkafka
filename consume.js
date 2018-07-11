'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const genId = require('./generate-id');

function consume(options, callback) {
    const {
        topicName,
        updateOffsets,
        shouldFinish,
        processedMessage,
        kafkaBrokers,
        startTimeout
    } = options;

    let rebalancing = true;
    let randomTimeoutId;

    let assignments = [];
    let processed = 0;
    const offsets = {};
    const consumerDone = _.once(_consumerDone);

    if (!topicName) {
        console.error('requires a topicName'); // eslint-disable-line no-console
        process.exit(1);
    }

    debug('initializing...');
    const finishInterval = setInterval(() => {
        updateOffsets(offsets);
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
        rebalance_cb(err, assignment) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                rebalancing = false;
                this.resume(assignment);

                debug('assignment', assignment);
                assignments = _.unionBy(assignments, ['partition', 'topic']);

                this.assign(assignment);
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                rebalancing = true;

                this.pause(assignment);

                debug('unassigned', assignment);
                assignments = _.without(assignments, assignment);

                this.unassign(assignment);
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
            consumer.consume();
        }, startTimeout);

        randomlyPauseAndResume();
    });

    consumer.on('data', (m) => {
        if (ended) {
            return;
        }
        const partition = _.toInteger(m.partition);
        offsets[`${partition}`] = m.offset;

        // committing offsets every numMessages
        if (offsets[partition] % (numMessages + 1) === numMessages) {
            updateOffsets(offsets);
            if (rebalancing) {
                debug('is rebalancing');
            }
            consumer.commit(m);
        }
        processed += 1;
        processedMessage({ partition });
    });

    consumer.on('disconnected', () => {
        debug('consumer disconnected');
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
        clearTimeout(randomTimeoutId);
        clearInterval(finishInterval);
        ended = true;
        debug('offsets', offsets);
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
        const timeout = _.random(1000, 10000);
        debug(`will pause and resume in ${timeout}ms`);
        randomTimeoutId = setTimeout(() => {
            const resumeTimeout = _.random(100, 3000);
            debug(`pausing assignedPartitions, will resume in ${resumeTimeout}ms`, assignments);
            consumer.pause(assignments);
            randomTimeoutId = setTimeout(() => {
                consumer.resume(assignments);
                randomlyPauseAndResume();
            }, resumeTimeout);
        }, timeout);
    }
}

module.exports = consume;
