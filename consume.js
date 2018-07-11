'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const signale = require('signale');

function consume(options, callback) {
    const {
        key,
        topicName,
        updateOffsets,
        shouldFinish,
        processedMessage,
        kafkaBrokers
    } = options;

    const logger = signale.scope(key);
    let rebalancing = true; // eslint-disable-line

    let assignments = [];
    let processed = 0;
    const offsets = {};
    const consumerDone = _.once(_consumerDone);

    if (!topicName) {
        logger.error('requires a topicName');
        process.exit(1);
    }

    logger.info('initializing...');
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
                logger.info(`assigned ${JSON.stringify(newPartitions)}`);
                _.each(newPartitions, (partition) => {
                    if (!offsets[partition]) {
                        offsets[`${partition}`] = 0;
                    }
                });
                this.assign(assignment);
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                rebalancing = true;
                const removedPartitions = _.map(assignment, r => _.toInteger(r.partition));
                logger.info(`unassigned ${JSON.stringify(removedPartitions)}`);
                assignments = _.without(assignments, ...removedPartitions);
                _.each(removedPartitions, (partition) => {
                    delete offsets[`${partition}`];
                });
                this.unassign(assignment);
            } else {
                // We had a real error
                logger.error(err);
            }
        }
    });

    let ended = false;

    // logging debug messages, if debug is enabled
    consumer.on('event.log', (log) => {
        if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
            logger.debug(log.message);
        }
    });

    // logging all errors
    consumer.on('event.error', (err) => {
        logger.error(err);
    });

    const numMessages = 1000;

    consumer.on('ready', () => {
        logger.info('ready!');

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
            logger.error(err);
        }
        logger.info('connected');
    });

    function _consumerDone(err) {
        logger.info('done!');
        clearInterval(finishInterval);
        ended = true;
        logger.info(`offsets: ${JSON.stringify(offsets, null, 2)}`);
        if (consumer.isConnected()) {
            consumer.disconnect();
        }
        if (err) {
            callback(err);
            return;
        }
        logger.success(`processed ${processed}`);
        callback(null);
    }
}

module.exports = consume;
