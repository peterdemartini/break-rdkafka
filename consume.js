const Kafka = require('node-rdkafka')
const _ = require('lodash')

function consume ({ logger, topicName, shouldFinish, kafkaBrokers }, callback) {
  if (!topicName) {
    logger.error(`requires a topicName`)
    process.exit(1)
  }
  logger.info(`initializing...`)
  let processed = 0
  const consumerDone = _.once((err) => {
    ended = true
    logger.info(`offsets: ` + JSON.stringify(offsets, null, 2))
    if (consumer.isConnected()) {
      consumer.disconnect()
    }
    if (err) {
      callback(err, offsets)
      return
    }
    logger.success(`processed ${processed}`)
    callback(null, offsets)
  })
  let assignments = []
  const consumer = new Kafka.KafkaConsumer({
    'client.id': _.uniqueId('break-kafka-'),
    'metadata.broker.list': kafkaBrokers,
    'group.id': topicName + '-group',
    'enable.auto.commit': false,
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'beginning',
    'rebalance_cb': function (err, assignment) {
      if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
        const newPartitions = _.map(assignment, (r) => parseInt(r.partition, 10))
        assignments = _.union(assignments, newPartitions)
        logger.info(`assigned ${JSON.stringify(newPartitions)}`)
        this.assign(assignment)
      } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
        const removedPartitions = _.map(assignment, (r) => parseInt(r.partition, 10))
        logger.info(`unassigned ${JSON.stringify(removedPartitions)}`)
        assignments = _.without(assignments, ...removedPartitions)
        this.unassign(assignment)
      } else {
        // We had a real error
        logger.error(err)
      }
    }
  })

  const offsets = {}
  let ended = false

  // logging debug messages, if debug is enabled
  consumer.on('event.log', function (log) {
    logger.log(`${log}`)
  })

  // logging all errors
  consumer.on('event.error', function (err) {
    consumerDone(err)
  })

  const numMessages = 1000

  consumer.on('ready', function (arg) {
    logger.info(`ready!`)

    consumer.subscribe([topicName])
    // start consuming messages
    consumer.consume()
  })

  consumer.on('data', function (m) {
    if (ended) {
      return
    }
    const partition = parseInt(m.partition, 10)
    _.set(offsets, partition, m.offset)

    // committing offsets every numMessages
    if (offsets[partition] % (numMessages + 1) === numMessages) {
      logger.pending(`processed ${processed}`)
      consumer.commit(m)
    }
    _.forEach(assignments, (par) => {
      if (par === partition) {
        return
      }
      const offset = _.get(offsets, par, 0)
      if (offset < 0) return
      const range = 1000
      const upper = offset + range
      const lower = offset - range
      if (!_.inRange(offsets[partition], lower, upper)) {
        logger.error(`Expected partition #${partition} (${m.offset}) of to be within range +/- ${range} of partition of #${par} (${offset}).`)
      }
    })
    processed++
    if (shouldFinish()) {
      consumerDone()
    }
  })

  consumer.on('disconnected', function (arg) {
    consumerDone(new Error('Consumer Disconnected'))
  })

  // starting the consumer
  consumer.connect({}, (err) => {
    if (err) {
      logger.error(err)
    }
    logger.info('connected')
  })
}

module.exports = consume
