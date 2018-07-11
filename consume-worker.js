'use strict';

const consume = require('./consume');

const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const startTimeout = parseInt(process.env.START_TIMEOUT, 10);

let finished = false;

process.on('message', ({ fn }) => {
    if (fn === 'shouldFinish') {
        finished = true;
    }
});

const updateAssignments = (assignments) => {
    process.send({ fn: 'updateAssignments', assignments });
};

const processedMessage = (msg) => {
    process.send({ fn: 'processedMessage', msg });
};

const shouldFinish = () => finished;

consume({
    topicName,
    updateAssignments,
    processedMessage,
    shouldFinish,
    kafkaBrokers,
    startTimeout
}, (err) => {
    if (err) {
        throw err;
    }
    process.exit();
});
