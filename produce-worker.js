'use strict';

const genId = require('./generate-id');
const produce = require('./produce');

const producedMessages = (msg) => {
    process.send({ fn: 'producedMessages', msg });
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
    if (fn === 'producedMessages') {
        producedMessages(msg);
    }
});

produce({
    producedMessages,
    startBatch,
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
