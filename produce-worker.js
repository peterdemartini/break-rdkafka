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

setInterval(() => {
    process.send({ fn: 'heartbeat', validFor: 6000 });
}, 2000).unref();

const reportError = (error) => {
    process.send({ fn: 'reportError', error: error.stack ? error.stack : error.toString() });
};

produce({
    producedMessages,
    startBatch,
    reportError,
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
