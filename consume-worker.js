'use strict';

const debug = require('debug')(`break-rdkafka:${process.env.BREAK_KAFKA_KEY}`);
const consume = require('./consume');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const startTimeout = parseInt(process.env.START_TIMEOUT, 10);

let finished = false;
let ready = false;

process.on('message', ({ fn }) => {
    if (fn === 'shouldFinish') {
        if (!ready) {
            console.error('Told to finish before ready'); // eslint-disable-line
            process.exit(1);
        }
        finished = true;
    }
});

debug(`Waiting for ${startTimeout}ms before starting...`);

setTimeout(() => {
    ready = true;
    process.send({ fn: 'ready' });

    const updateOffsets = (offsets) => {
        process.send({ fn: 'updateOffsets', offsets });
    };

    const processedMessage = (msg) => {
        process.send({ fn: 'processedMessage', msg });
    };

    const shouldFinish = () => finished;


    consume({
        topicName,
        updateOffsets,
        processedMessage,
        shouldFinish,
        kafkaBrokers
    }, (err) => {
        if (err) {
            throw err;
        }
        process.exit();
    });
}, startTimeout);
