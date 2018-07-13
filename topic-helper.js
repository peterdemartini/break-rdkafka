'use strict';

const Promise = require('bluebird');
const debug = require('debug')('break-rdkafka');

async function ensureTopic(client, topic, numPartitions) {
    try {
        await deleteTopic(client, topic);
    } catch (err) {
        // this error is probably okay
        debug('unable to delete topic', err);
    }
    await Promise.delay(1000);
    await createTopic(client, topic, numPartitions);
}

function createTopic(client, topic, numPartitions) {
    const _createTopic = Promise.promisify(client.zk.createTopic, { context: client.zk });
    return _createTopic(topic, numPartitions, 1, {});
}

function deleteTopic(client, topic) {
    const _deleteTopics = Promise.promisify(client.zk.deleteTopics, { context: client.zk });
    return _deleteTopics([topic]);
}

module.exports = {
    createTopic,
    deleteTopic,
    ensureTopic
};
