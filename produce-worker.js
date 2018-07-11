'use strict';

const genId = require('./generate-id');
const produce = require('./produce');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;

const sentMessage = (msg) => {
    process.send({ fn: 'sentMessage', msg });
};

const startBatch = (callback) => {
    const requestId = genId('batch');
    process.send({ fn: 'getMessageBatch', requestId });
    const onMessage = ({ fn, messages, responseId }) => {
        if (fn !== 'receiveMessageBatch') {
            return;
        }
        if (requestId !== responseId) {
            return;
        }
        process.removeListener('message', onMessage);
        callback(null, messages);
    };
    process.on('message', onMessage);
};

process.on('message', ({ fn, msg }) => {
    if (fn === 'sentMessage') {
        sentMessage(msg);
    }
});

produce({
    topicName,
    sentMessage,
    startBatch,
    kafkaBrokers
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
