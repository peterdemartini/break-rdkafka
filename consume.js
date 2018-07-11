'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);

function consume(options, callback) {
    const {
        topicName,
        updateOffsets,
        shouldFinish,
        processedMessage,
        kafkaBrokers
    } = options;

    let rebalancing = true;

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
            consumerDone();
        }
    }, 500);


    const consumer = new Kafka.KafkaConsumer({
        'client.id': _.uniqueId('break-kafka-'),
        debug: 'cgrp,topic',
        'metadata.broker.list': kafkaBrokers,
        'group.id': `${topicName}-group`,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        'auto.offset.reset': 'beginning',
        rebalance_cb(err, assignment) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                rebalancing = false;
                const newPartitions = _.map(assignment, r => _.toInteger(r.partition));
                assignments = _.union(assignments, newPartitions);
                debug(`assigned ${JSON.stringify(newPartitions)}`);
                _.each(newPartitions, (partition) => {
                    if (!offsets[partition]) {
                        offsets[`${partition}`] = 0;
                    }
                });
                this.assign(assignment);
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                rebalancing = true;
                const removedPartitions = _.map(assignment, r => _.toInteger(r.partition));
                debug(`unassigned ${JSON.stringify(removedPartitions)}`);
                assignments = _.without(assignments, ...removedPartitions);
                _.each(removedPartitions, (partition) => {
                    delete offsets[`${partition}`];
                });
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

        consumer.subscribe([topicName]);
        // start consuming messages
        consumer.consume();
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
        consumerDone(new Error('Consumer Disconnected'));
    });

    // starting the consumer
    consumer.connect({}, (err) => {
        if (err) {
            console.error(err); // eslint-disable-line no-console
        }
        debug('connected');
    });

    function _consumerDone(err) {
        debug('done!');
        clearInterval(finishInterval);
        ended = true;
        debug(`offsets: ${JSON.stringify(offsets, null, 2)}`);
        if (consumer.isConnected()) {
            consumer.disconnect();
        }
        if (err) {
            callback(err);
            return;
        }
        debug(`processed ${processed}`);
        callback(null);
    }
}

module.exports = consume;
