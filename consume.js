'use strict';

const consumerId = process.env.CONSUMER_ID;

const Promise = require('bluebird');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const Kafka = require('node-rdkafka');
const debug = require('debug')(`break-rdkafka:${consumerId}`);
const exitHandler = require('./exit-handler');

// const breakKafka = process.env.BREAK_KAFKA === 'true';
const enableRebalance = process.env.ENABLE_REBALANCE === 'true';
const assignedPartitions = JSON.parse(process.env.ASSIGNED_PARTITIONS);
const useCommitSync = process.env.USE_COMMIT_SYNC === 'true';
const topicName = process.env.BREAK_KAFKA_TOPIC_NAME;
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS;
const enablePauseAndResume = process.env.ENABLE_PAUSE_AND_RESUME === 'true';
const useConsumerPauseAndResume = process.env.USE_CONSUMER_PAUSE_AND_RESUME === 'true';
const startTimeout = parseInt(process.env.START_TIMEOUT, 10);
const batchSize = parseInt(process.env.BATCH_SIZE, 10);

const statsFile = path.join(__dirname, 'stats', `${consumerId}.json`);
try {
    fs.unlinkSync(statsFile);
} catch (err) {
    // this is okay
}

const heartbeatInterval = setInterval(() => {
    process.send({ fn: 'heartbeat', validFor: 10000 });
}, 2000).unref();

const updateAssignments = (assignments) => {
    process.send({ fn: 'updateAssignments', assignments });
};

const reportError = (error) => {
    process.send({ fn: 'reportError', error: error.stack ? error.stack : error.toString() });
};

const consumedMessages = (msg) => {
    process.send({ fn: 'consumedMessages', msg });
};

let paused = false;
let rebalancing = false;
let randomTimeoutId;

let assignments = [];
let consumeTimeout;
let processed = 0;
let ended = false;
let processing = false;

function simpleTopics(input) {
    let topics = input;
    if (_.isPlainObject(input)) {
        topics = _.values(input);
    }
    const formatLong = _.size(topics) > 3;
    const sep = formatLong ? '\n - ' : '; ';
    const prefix = formatLong ? sep : '';
    return prefix + _.map(topics, ({ partition, offset = 'N/A' }) => {
        const message = `partition: ${partition}, offset: ${offset}`;
        return message;
    }).join(sep);
}

debug('initializing...');
const finishInterval = setInterval(() => {
    if (ended) return;
    updateAssignments(assignments);
}, 500);

const consumer = new Kafka.KafkaConsumer({
    'client.id': consumerId,
    debug: 'consumer,cgrp,topic,fetch',
    // 'session.timeout.ms': breakKafka ? 10000 : 1000,
    'fetch.wait.max.ms': 1000,
    'metadata.broker.list': kafkaBrokers,
    'group.id': `${topicName}-group`,
    'enable.auto.commit': false,
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'smallest',
    'statistics.interval.ms': 5000,
    rebalance_cb(err, changed) {
        if (!enableRebalance) {
            if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                debug('REBALANCE_DISABLED: assigned', simpleTopics(changed));
                manuallyAssign();
            } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                debug('REBALANCE_DISABLED: unassigned', simpleTopics(changed));
                manuallyAssign();
            } else {
                // We had a real error
                reportError(err);
            }
            return;
        }
        if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
            rebalancing = false;
            const updated = _.filter(changed, ({ partition }) => {
                const isNew = !_.some(assignments, { partition });
                return isNew;
            });
            assignments = _.unionBy(assignments, updated, 'partition');
            debug('REBALANCE: assigned', simpleTopics(assignments));

            this.assign(changed);
        } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
            rebalancing = true;
            debug('REBALANCE: unassigned', simpleTopics(changed));

            this.unassign(changed);
        } else {
            // We had a real error
            reportError(err);
        }
    },
    offset_commit_cb(err, topicPartitions) {
        if (err) {
            // There was an error committing
            reportError(err);
        } else {
            _.forEach(topicPartitions, ({ partition, offset }) => {
                assignments = _.map(assignments, (assigned) => {
                    if (assigned.partition === partition) {
                        assigned.offsets = offset;
                    }
                    return assigned;
                });
            });
            // Commit went through. Let's log the topic partitions
            debug('COMMIT DONE', simpleTopics(topicPartitions));
        }
    },
});

    // logging debug messages, if debug is enabled
consumer.on('event.log', (log) => {
    if (/(fail|error|warn|issue|disconnect|problem|unable|invalid|rebalance)/gi.test(log.message)) {
        debug('DEBUG', log.message);
    }
});

// logging all errors
consumer.on('event.error', (err) => {
    reportError(err);
});

consumer.on('event.throttle', (err) => {
    debug(err);
    reportError(err);
});

consumer.on('event.stats', ({ message }) => {
    try {
        const stats = JSON.parse(message);
        stats.COLLECTED_AT = new Date().toString();
        fs.writeFileSync(statsFile, JSON.stringify(stats, null, 2));
    } catch (err) {
        reportError(err);
    }
});


consumer.on('ready', () => {
    debug('ready!');

    debug(`Waiting for ${startTimeout}ms before starting...`);
    consumer.subscribe([topicName]);
    manuallyAssign();

    setTimeout(() => {
        debug('Starting...');
        // start consuming messages
        consume();
        if (enablePauseAndResume) randomlyPauseAndResume();
    }, startTimeout);
});

function manuallyAssign() {
    if (enableRebalance) return;

    debug('MANUALLY ASSIGNING PARTITIONS: ', assignedPartitions);
    const topics = _.map(assignedPartitions, partition => new Kafka.TopicPartition(topicName, partition));
    consumer.assign(topics);
}

function consume() {
    processing = false;
    clearTimeout(consumeTimeout);
    if (ended) {
        return;
    }
    consumeTimeout = setTimeout(() => {
        if (paused || rebalancing) {
            consume();
            return;
        }
        processing = true;
        consumer.consume(batchSize, (err, messages) => {
            if (err) {
                reportError(err);
                consume();
                return;
            }

            if (_.isEmpty(messages)) {
                consume();
                return;
            }

            const offsets = {};

            _.forEach(messages, (message) => {
                const { offset, partition } = message;
                assignments = _.map(assignments, (assignment) => {
                    if (assignment.partition === partition) {
                        assignment.offset = offset;
                    }
                    return assignment;
                });
                const current = _.get(offsets, [partition, 'offset'], 0);
                if (offset > current) {
                    _.set(offsets, partition, message);
                }
                processed += 1;
            });

            if (paused || rebalancing) {
                reportError('WARNING: about to commit when paused or rebalancing');
            }

            _.forEach(_.values(offsets), ({ offset: _offset, topic, partition }) => {
                const offset = _offset + 1;
                if (useCommitSync) {
                    debug(`committing sync... partition: ${partition} offset: ${offset}`);
                    consumer.commitSync({
                        offset,
                        partition,
                        topic
                    });
                    debug('committing took');
                } else {
                    consumer.commit({
                        offset,
                        partition,
                        topic
                    });
                }
            });

            consumedMessages(messages);
            consume();
        });
    }, 1000);
}


consumer.on('disconnected', () => {
    if (ended) return;
    consumerDone(new Error('Consumer Disconnected'));
});

// starting the consumer
consumer.connect({}, (err) => {
    if (err) {
        reportError(err);
        return;
    }
    debug('connected');
});

function randomlyPauseAndResume() {
    if (_.isEmpty(assignments)) {
        randomTimeoutId = setTimeout(() => {
            randomlyPauseAndResume();
        }, 1000);
        return;
    }

    if (_.random(0, 5)) {
        randomTimeoutId = setTimeout(() => {
            randomlyPauseAndResume();
        }, 5000);
        return;
    }

    const pauseTimeout = _.random(1000, 30 * 1000);
    const resumeTimeout = _.random(1000, 15 * 1000);
    debug(`CHOAS: will pause in ${pauseTimeout}ms and resume in ${resumeTimeout}ms`);

    randomTimeoutId = setTimeout(() => {
        pause();

        randomTimeoutId = setTimeout(() => {
            resume();
            randomlyPauseAndResume();
        }, resumeTimeout);
    }, pauseTimeout);
}

function pause() {
    if (!enablePauseAndResume) return;
    if (paused) return;
    debug('PAUSING!');
    if (useConsumerPauseAndResume) {
        consumer.pause(assignments);
    }
    paused = true;
}

function resume() {
    if (!enablePauseAndResume) return;
    if (!paused) return;
    debug('RESUMING!');
    if (useConsumerPauseAndResume) {
        consumer.resume(assignments);
    }
    paused = false;
}

async function checkProcessing() {
    if (!processing) {
        return;
    }
    await Promise.delay(100);
    await checkProcessing();
}

async function checkDisconnect() {
    if (!consumer.isConnected()) {
        return;
    }
    await Promise.delay(100);
    await checkDisconnect();
}

async function consumerDone(err, skipExit) {
    if (ended && err) {
        console.error('GOT ERROR after shutdown', err); // eslint-disable-line no-console
        return;
    }

    clearTimeout(consumeTimeout);
    clearTimeout(randomTimeoutId);
    clearInterval(finishInterval);

    ended = true;

    if (consumer.isConnected()) {
        await checkProcessing();
    }

    consumer.disconnect();

    await checkDisconnect();

    clearInterval(heartbeatInterval);

    debug('assignments', simpleTopics(assignments));
    debug('processed', processed);

    if (skipExit) return;

    if (err) {
        console.error(err); // eslint-disable-line no-console
        process.exit(1);
    }
    process.exit(0);
}

exitHandler((signal, err) => consumerDone(err, true), 5 * 1000);
