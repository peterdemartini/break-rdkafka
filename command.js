'use strict';

const _ = require('lodash');
const { kafka } = require('kafka-tools');
const signale = require('signale');
const stringifyObject = require('stringify-object');
const debug = require('debug')('break-rdkafka');

const exitHandler = require('./exit-handler');
const genId = require('./generate-id');
const { ensureTopic, deleteTopic } = require('./topic-helper');
const {
    createConsumer,
    createProducer,
    killAll,
    hasChildren,
    countChildren,
    getAssignments
} = require('./child-helper');

const breakKafka = process.env.BREAK_KAFKA === 'true';

const {
    KAFKA_BROKERS = 'localhost:9092,localhost:9093',
    NUM_CONSUMERS = '4',
    NUM_PRODUCERS = '3',
    NUM_PARTITIONS = '15',
    MESSAGES_PER_PARTITION = '30000',
    DEBUG = 'break-rdkafka*',
    ENABLE_PAUSE_AND_RESUME = breakKafka ? 'true' : 'false',
    USE_CONSUMER_PAUSE_AND_RESUME = 'true',
    START_TIMEOUT = breakKafka ? '5000' : '1000',
    USE_COMMIT_SYNC = breakKafka ? 'true' : 'false',
    BATCH_SIZE = '10000',
} = process.env;

const kafkaBrokers = KAFKA_BROKERS;
const numProducers = _.toSafeInteger(NUM_PRODUCERS);
const numConsumers = _.toSafeInteger(NUM_CONSUMERS);
const numPartitions = _.toSafeInteger(NUM_PARTITIONS);
const messagesPerPartition = _.toSafeInteger(MESSAGES_PER_PARTITION);
const startTimeout = _.toSafeInteger(START_TIMEOUT);
const batchSize = _.toSafeInteger(BATCH_SIZE);

async function run() {
    const topicName = 'break-kafka-topic';

    if (breakKafka) {
        debug('WARNING: CONFIGURED TO BREAK KAFKA');
    }

    debug('initializing...', {
        topicName,
        numPartitions,
        messagesPerPartition,
        numConsumers,
        numProducers,
        batchSize,
        startTimeout,
        ENABLE_PAUSE_AND_RESUME,
        USE_COMMIT_SYNC,
        USE_CONSUMER_PAUSE_AND_RESUME,
    });

    const messages = makeMessages();
    const totalMessages = _.size(messages);

    const client = new kafka.Client('localhost:2181', genId('break-kafka-'), {
        sessionTimeout: 5000,
        spinDelay: 500,
        retries: 0
    });

    await ensureTopic(client, topicName, numPartitions);

    const produced = {};
    const consumed = {};

    _.times(numPartitions, (partition) => {
        consumed[`${partition}`] = 0;
        produced[`${partition}`] = 0;
    });

    let lastConsumedCount = -1;
    let lastProducedCount = -1;
    let consumeStuckCount = 0;
    let produceStuckCount = 0;
    let exiting = false;
    const reportedErrors = [];

    function getConsumedCount() {
        return _.sum(_.values(consumed));
    }

    function getProducedCount() {
        return _.sum(_.values(produced));
    }

    function isConsuming() {
        const count = getConsumedCount();
        return count > 0 && count < totalMessages;
    }

    function isProducing() {
        const count = getProducedCount();
        return count > 0 && count < totalMessages;
    }

    function isDoneConsuming() {
        return getConsumedCount() >= totalMessages;
    }

    function isDoneProducing() {
        return getProducedCount() >= totalMessages && _.isEmpty(messages);
    }

    function isExiting() {
        return exiting;
    }

    function reportError(input) {
        let message;
        if (_.isString(input)) {
            message = input;
        } else {
            message = input.stack ? input.stack : input.toString();
        }
        debug(message);
        reportedErrors.push(message);
    }

    function getBatch() {
        const remaining = _.size(messages);
        const count = remaining > batchSize ? batchSize : remaining;
        const randomStart = _.random(0, remaining);
        return messages.splice(randomStart, count);
    }

    const updateInterval = setInterval(() => {
        const consumedCount = getConsumedCount();
        const producedCount = getProducedCount();
        const childrenCount = countChildren();
        const updates = [];

        if (!isProducing()) {
            updates.push('waiting for messages to be produced...');
        } else if (isDoneProducing()) {
            updates.push(`all ${producedCount} messages have been produced`);
        } else {
            const remaining = totalMessages - producedCount;
            updates.push(`produced ${producedCount - lastProducedCount}, ${remaining} remaining...`);
        }

        if (!isConsuming()) {
            updates.push('waiting for messages to be consumed...');
        } else if (isDoneConsuming()) {
            updates.push(`all ${consumedCount} messages have been consumed`);
        } else {
            const remaining = totalMessages - consumedCount;
            updates.push(`consumed ${consumedCount - lastConsumedCount}, ${remaining} remaining...`);
        }

        updates.push(`active child processes ${childrenCount}`);

        if (isConsuming() && consumedCount === lastConsumedCount) {
            reportError(`WARNING: consumed count ${producedCount} stayed the same`);
            consumeStuckCount += 1;
        } else {
            consumeStuckCount = 0;
        }

        if (isProducing() && producedCount === lastProducedCount) {
            reportError(`WARNING: produced count ${producedCount} stayed the same`);
            produceStuckCount += 1;
        } else {
            produceStuckCount = 0;
        }

        const errCount = _.size(reportedErrors);
        if (!errCount) {
            updates.push('no reported errors');
        } else {
            updates.push(...reportedErrors);
        }

        debug(`UPDATES:\n - ${updates.join('\n - ')}\n`);

        if (consumeStuckCount === 3) {
            exit(new Error(`Consume count (${consumedCount}) has stayed the same for 30 seconds`));
        } else if (produceStuckCount === 3) {
            exit(new Error(`Produce count (${producedCount}) has stayed the same for 30 seconds`));
        } else {
            lastConsumedCount = consumedCount;
            lastProducedCount = producedCount;
        }
    }, 10 * 1000);

    async function exit(err, signal, skipExit) {
        if (exiting) {
            if (err) {
                reportError(err);
            }
            debug('already exiting...');
            return;
        }

        if (err) {
            signale.error('Will exit because to error: ', err);
        }

        clearInterval(updateInterval);
        debug('Exiting in 5 seconds...');

        exiting = true;
        await Promise.delay(5000);
        debug('Exiting now...');
        try {
            await killAll(signal);
        } catch (killAllErr) {
            signale.error(killAllErr);
        }

        try {
            await deleteTopic(topicName);
        } catch (dErr) {
            signale.error(dErr);
        }

        debug(`DELETED TOPIC: ${topicName}`);
        debug('assignments', stringifyObject(getAssignments()));
        debug('produced', stringifyObject(produced));
        debug('consumed', stringifyObject(consumed));
        debug('reportedErrors', stringifyObject(reportedErrors));

        if (err) {
            signale.fatal(err);
            if (skipExit) return;
            process.exit(1);
        }

        signale.success('DONE!');

        if (skipExit) return;
        process.exit(0);
    }

    async function exitIfNeeded() {
        if (exiting) return;

        if (isDoneConsuming() && isDoneProducing()) {
            debug('is done consuming and producing, exiting...');
            await exit();
        }

        if (hasChildren()) {
            debug('No more children left, exiting...');
            await exit();
        }
    }

    const debugToManyMessages = _.debounce((count) => {
        debug(`consumed more messages than it should have (${count})`);
    }, 1000);

    function consumedMessages(input) {
        _.forEach(input, (message) => {
            consumed[`${message.partition}`] += 1;
        });
        const count = getConsumedCount();
        if (count === totalMessages) {
            signale.success(`consumed all of ${totalMessages} messages`);
            debug('consumed result', consumed);
            exit();
        }
        if (count > totalMessages) {
            debugToManyMessages(count);
        }
    }

    function producedMessages(input) {
        _.forEach(input, (message) => {
            produced[`${message.partition}`] += 1;
        });
        const count = getProducedCount();
        if (count === totalMessages) {
            signale.success(`produced all of ${totalMessages} messages`);
            debug('produced result', produced);
        }
        if (count > totalMessages) {
            reportError(`produced more messages than it should have (${count})`);
        }
    }


    _.times(numProducers, () => {
        const config = {
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
        };
        createProducer(config);
    });

    _.times(numConsumers, () => {
        const config = {
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
        };
        createConsumer(config);
    });

    exitHandler(signal => exit(null, signal, true));

    function makeMessages() {
        signale.time('making messages');
        const results = [];
        _.times(numPartitions, (partition) => {
            const partionMessages = _.times(messagesPerPartition, () => ({ partition }));
            results.push(...partionMessages);
        });
        signale.timeEnd('making messages');
        debug(`created ${_.size(results)}`);
        return results;
    }
}

run();
