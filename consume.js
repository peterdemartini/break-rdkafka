const Kafka = require('node-rdkafka')
const _ = require('lodash')
const signale = require('signale')

function consume ({ id, topicName, numPartitions, maxMessages, kafkaBrokers }) {
  if (!topicName) {
    signale.error(`[${id}] requires a topicName`)
    process.exit(1)
  }
  if (!numPartitions) {
    signale.error(`[${id}] requires a numPartitions`)
    process.exit(1)
  }
  if (!maxMessages) {
    signale.error(`[${id}] requires a maxMessages`)
    process.exit(1)
  }
  const consumer = new Kafka.KafkaConsumer({
    'metadata.broker.list': kafkaBrokers,
    'group.id': topicName + '-group',
    'enable.auto.commit': false,
    'enable.auto.offset.store': false,
    'auto.offset.reset': 'beginning'
  })

  const offsets = {}
  let ended = false
  function end () {
    ended = true
    signale.info(`[${id}] offsets: ` + JSON.stringify(offsets, null, 2))
    signale.timeEnd(`[${id}]`)
    consumer.disconnect()
    signale.info(`[${id}] Exiting...`)
    process.exit(1)
  }

  // logging debug messages, if debug is enabled
  consumer.on('event.log', function (log) {
    signale.log(`[${id}] ${log}`)
  })

  // logging all errors
  consumer.on('event.error', function (err) {
    signale.error(`[${id}] Error from consumer`)
    signale.error(err)
    end()
  })

  const numMessages = 1000

  const endAfter = _.after(maxMessages, () => {
    signale.success(`[${id}] Done processing ${maxMessages}`)
    end()
  })

  consumer.on('ready', function (arg) {
    signale.time(`[${id}]`)
    signale.info(`[${id}] Starting... topic: ${topicName} maxMessages: ${maxMessages}`)

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
      consumer.commit(m)
    }
    const assignments = (() => {
      const results = consumer.assignments()
      if (!_.isArray(results)) {
        return []
      }
      return _.map(results, (r) => parseInt(r.partition, 10))
    })()
    _.forEach(assignments, (par) => {
      if (par === partition) {
        return
      }
      const offset = _.get(offsets, par, 0)
      const range = 1000
      const upper = offset + range
      const lower = offset - range
      if (!_.inRange(offsets[partition], lower, upper)) {
        signale.error(`[${id}] Expected partition #${partition} (${m.offset}) of to be within range +/- ${range} of partition of #${par} (${offset}).`)
      }
    })
    endAfter()
  })

  consumer.on('disconnected', function (arg) {
    signale.error(`[${id}] disconnected. ${JSON.stringify(arg)}`)
    end()
  })

  // starting the consumer
  consumer.connect()
}

module.exports = consume
