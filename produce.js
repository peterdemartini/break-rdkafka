'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const uuidv4 = require('uuid/v4');
const signale = require('signale');

function produce(options, callback) {
    const {
        key,
        topicName,
        sentMessage,
        startBatch,
        kafkaBrokers
    } = options;

    const logger = signale.scope(key);
    if (!topicName) {
        logger.error('requires a topicName');
        process.exit(1);
    }

    logger.info('initializing...');

    let ended = false;
    const producerDone = _.once(_producerDone);

    const producer = new Kafka.Producer({
        'client.id': _.uniqueId('break-kafka-'),
        debug: 'broker,topic',
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.ms': 1000,
        'batch.num.messages': 100000,
        'metadata.broker.list': kafkaBrokers
    });

    // logging debug messages, if debug is enabled
    producer.on('event.log', (log) => {
        if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
            logger.debug(log.message);
        }
    });

    producer.setPollInterval(100);

    // logging all errors
    producer.on('event.error', (err) => {
        logger.error(err);
    });

    // Wait for the ready event before producing
    producer.on('ready', () => {
        logger.info('ready!');
        const sendMessages = () => {
            startBatch((err, messages) => {
                if (err) {
                    producerDone(err);
                    return;
                }
                if (_.isEmpty(messages)) {
                    producerDone();
                    return;
                }
                const sendAfter = _.after(_.size(messages), () => {
                    sendMessages();
                });
                _.forEach(messages, (message) => {
                    if (ended) {
                        return;
                    }
                    const value = Buffer.from(uuidv4());
                    const result = producer.produce(
                        topicName,
                        message.partition,
                        value,
                        message.key
                    );
                    if (result !== true) {
                        logger.warn(`produce did not return true, got ${result}`);
                    }
                    sentMessage(message);
                    sendAfter();
                });
            });
        };
        sendMessages();
    });

    producer.on('disconnected', () => {
        producerDone(new Error('Producer Disconnected'));
    });

    // starting the producer
    producer.connect();

    function _producerDone(err) {
        logger.info('done!');
        ended = true;
        if (producer.isConnected()) {
            producer.disconnect();
        }
        if (err) {
            callback(err);
            return;
        }
        callback();
    }
}

module.exports = produce;
