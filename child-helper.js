'use strict';

const _ = require('lodash');
const signale = require('signale');
const { fork } = require('child_process');
const debug = require('debug')('break-rdkafka');

const children = {};
const assignments = {};

function createProducer(config) {
    const {
        topicName,
        kafkaBrokers,
        batchSize,
        DEBUG,
        isDoneProducing,
        isExiting,
        reportError,
        exitIfNeeded,
        producedMessages,
        getBatch,
    } = config;
    let recreating = false;
    const producerId = _.uniqueId('produce-');
    let heartbeatTimeout;
    signale.time(producerId);

    const child = fork(`${__dirname}/produce-worker.js`, [], {
        env: {
            PRODUCER_ID: producerId,
            BREAK_KAFKA_TOPIC_NAME: topicName,
            BREAK_KAFKA_BROKERS: kafkaBrokers,
            BATCH_SIZE: batchSize,
            FORCE_COLOR: '1',
            DEBUG,
        },
        stdio: 'inherit'
    });

    child.on('close', (code) => {
        delete children[producerId];
        if (isDoneProducing() || !recreating) {
            clearTimeout(heartbeatTimeout);
        }

        if (!isExiting() && isDoneProducing()) {
            const message = `WARNING: child ${producerId} exited with status code ${code}`;
            reportError(message);
            if (code !== 0) {
                signale.failure(`${producerId} died with an exit code of ${code}`);
                return;
            }
        }

        exitIfNeeded();
        signale.timeEnd(producerId);
        signale.success(`${producerId} done!`);
    });

    child.on('error', (err) => {
        signale.error(err);
    });

    child.on('message', (data) => {
        if (data.fn === 'producedMessages') {
            producedMessages(data.msg);
        }

        if (data.fn === 'getMessageBatch') {
            if (isDoneProducing()) {
                clearTimeout(heartbeatTimeout);
                recreating = false;
                debug(`Producing is done, sending SIGTERM to producer ${producerId}`);
                child.kill('SIGTERM');
                return;
            }
            const messages = getBatch();
            const responseId = data.requestId;
            child.send({ fn: 'receiveMessageBatch', messages, responseId });
        }

        if (data.fn === 'heartbeat') {
            clearTimeout(heartbeatTimeout);
            heartbeatTimeout = setTimeout(() => {
                if (!isExiting()) {
                    const message = `WARNING: ${producerId} hasn't responded to heartbeats in ${data.validFor}ms sending SIGKILL`;
                    debug(message);
                    reportError(message);
                }
                child.kill('SIGKILL');
                debug(`will recreating producer ${producerId} in ${data.validFor}ms...`);
                recreating = true;
                heartbeatTimeout = setTimeout(() => {
                    reportError(`INFO: recreating producer ${producerId}...`);
                    createProducer();
                }, data.validFor);
            }, data.validFor);
        }

        if (data.fn === 'reportError') {
            if (!isExiting()) {
                const message = `ERROR: ${producerId} reported error ${data.error}`;
                debug(message);
                reportError(message);
            }
        }
    });

    children[producerId] = child;
}

function createConsumer(config) {
    const {
        topicName,
        kafkaBrokers,
        batchSize,
        numConsumers,
        startTimeout,
        DEBUG,
        ENABLE_PAUSE_AND_RESUME,
        USE_COMMIT_SYNC,
        isDoneConsuming,
        isExiting,
        reportError,
        exitIfNeeded,
        consumedMessages,
    } = config;

    let recreating = false;
    const consumerId = _.uniqueId('consume-');
    let heartbeatTimeout;
    signale.time(consumerId);

    const child = fork(`${__dirname}/consume-worker.js`, [], {
        env: {
            CONSUMER_ID: consumerId,
            BREAK_KAFKA_TOPIC_NAME: topicName,
            BREAK_KAFKA_BROKERS: kafkaBrokers,
            BATCH_SIZE: batchSize,
            FORCE_COLOR: '1',
            START_TIMEOUT: _.random(0, numConsumers) * startTimeout,
            DEBUG,
            ENABLE_PAUSE_AND_RESUME,
            USE_COMMIT_SYNC
        },
        stdio: 'inherit'
    });

    child.on('close', (code) => {
        delete children[consumerId];
        if (isExiting() || (isExiting() && recreating)) {
            clearTimeout(heartbeatTimeout);
        }

        if (!isExiting() && isDoneConsuming()) {
            const message = `WARNING: child ${consumerId} exited with status code ${code}`;

            reportError(message);
            if (code > 0) {
                signale.failure(new Error(`${consumerId} died with an exit code of ${code}`));
                return;
            }
        }

        exitIfNeeded();
        signale.timeEnd(consumerId);
        signale.success(`${consumerId} done!`);
    });

    child.on('error', (err) => {
        signale.error(err);
    });

    child.on('message', (data) => {
        if (data.fn === 'consumedMessages') {
            consumedMessages(data.msg);
            if (isDoneConsuming()) {
                clearTimeout(heartbeatTimeout);
                recreating = false;
                debug(`Consuming is done, sending SIGTERM to consumer ${consumerId}`);
                child.kill('SIGTERM');
                return;
            }
        }

        if (data.fn === 'updateAssignments') {
            assignments[consumerId] = data.assignments;
        }

        if (data.fn === 'heartbeat') {
            clearTimeout(heartbeatTimeout);
            heartbeatTimeout = setTimeout(() => {
                if (!isExiting()) {
                    const message = `WARNING: ${consumerId} hasn't responded to heartbeats in ${data.validFor}ms sending SIGKILL`;
                    debug(message);
                    reportError(message);
                }
                child.kill('SIGKILL');
                debug(`will recreating consumer ${consumerId} in ${data.validFor}ms...`);
                recreating = true;
                heartbeatTimeout = setTimeout(() => {
                    reportError(`INFO: recreating consumer ${consumerId}...`);
                    createConsumer(config);
                }, data.validFor);
            }, data.validFor);
        }

        if (data.fn === 'reportError') {
            if (!isExiting()) {
                const message = `ERROR: ${consumerId} reported error ${data.error}`;
                reportError(message);
            }
        }
    });

    children[consumerId] = child;
}

async function killAll(signal = 'SIGTERM') {
    if (_.isEmpty(children)) {
        return;
    }
    const startTime = Date.now();
    signale.warn(`${signal} all remaining children`);

    _.forEach(_.values(children), (child) => {
        child.kill(signal);
    });

    async function waitUntilDead() {
        if (_.isEmpty(children)) return;

        debug('waiting until child processes are dead...', _.keys(children));

        if (startTime + 30000 > Date.now()) {
            throw new Error('Failed to shutdown children in 10 seconds');
        }

        await Promise.delay(1000);

        await waitUntilDead();
    }

    await waitUntilDead();
}

function hasChildren() {
    return _.isEmpty(children);
}

function getChildren() {
    return children;
}

function countChildren() {
    return _.size(_.values(children));
}

function getAssignments() {
    return assignments;
}

module.exports = {
    createConsumer,
    createProducer,
    killAll,
    getAssignments,
    getChildren,
    countChildren,
    hasChildren,
};
