'use strict';

const Kafka = require('node-rdkafka');
const _ = require('lodash');
const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const genId = require('./generate-id');

function produce(options, callback) {
    const {
        topicName,
        sentMessage,
        startBatch,
        kafkaBrokers
    } = options;


    if (!topicName) {
        console.error('requires a topicName'); // eslint-disable-line no-console
        process.exit(1);
    }

    debug('initializing...');

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
            debug(log.message);
        }
    });

    producer.setPollInterval(100);

    // logging all errors
    producer.on('event.error', (err) => {
        console.error(err); // eslint-disable-line no-console
    });

    // Wait for the ready event before producing
    producer.on('ready', () => {
        debug('ready!');
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
                    const value = Buffer.from(genId(message.key));
                    const result = producer.produce(
                        topicName,
                        message.partition,
                        value,
                        message.key
                    );
                    if (result !== true) {
                        debug(`produce did not return true, got ${result}`);
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
        debug('done!');
        ended = true;
        if (producer.isConnected()) {
            producer.disconnect();
        }
        if (err) {
            console.error(err); // eslint-disable-line no-console
            callback(err);
            return;
        }
        callback();
    }
}

module.exports = produce;
