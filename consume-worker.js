'use strict';

const consume = require('./consume');

let finished = false;

process.on('message', ({ fn }) => {
    if (fn === 'shouldFinish') {
        finished = true;
    }
});

const updateAssignments = (assignments) => {
    process.send({ fn: 'updateAssignments', assignments });
};

const consumedMessages = (msg) => {
    process.send({ fn: 'consumedMessages', msg });
};

const shouldFinish = () => finished;

consume({
    updateAssignments,
    consumedMessages,
    shouldFinish,
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
