'use strict';

const consume = require('./consume');

const updateAssignments = (assignments) => {
    process.send({ fn: 'updateAssignments', assignments });
};

const reportError = (error) => {
    process.send({ fn: 'reportError', error: error.stack ? error.stack : error.toString() });
};

const consumedMessages = (msg) => {
    process.send({ fn: 'consumedMessages', msg });
};

setInterval(() => {
    process.send({ fn: 'heartbeat', validFor: 9000 });
}, 3000).unref();

consume({
    updateAssignments,
    consumedMessages,
    reportError,
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
