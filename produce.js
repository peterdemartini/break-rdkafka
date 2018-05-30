const Kafka = require('node-rdkafka')
const _ = require('lodash')
const signale = require('signale')

function produce ({ key, topicName, numPartitions, sentMessage, getMessageBatch, kafkaBrokers }, callback) {
  const logger = signale.scope(key)
  if (!topicName) {
    logger.error(`requires a topicName`)
    process.exit(1)
  }

  if (!numPartitions) {
    logger.error(`requires a numPartitions`)
    process.exit(1)
  }

  logger.info(`initializing...`)

  let ended = false
  const producerDone = _.once((err) => {
    ended = true
    if (producer.isConnected()) {
      producer.disconnect()
    }
    if (err) {
      callback(err)
      return
    }
    callback()
  })

  const producer = new Kafka.Producer({
    'client.id': _.uniqueId('break-kafka-'),
    // 'debug' : 'all',
    'queue.buffering.max.messages': 500000,
    'queue.buffering.max.ms': 1000,
    'batch.num.messages': 100000,
    'metadata.broker.list': kafkaBrokers
  })

  // logging debug messages, if debug is enabled
  producer.on('event.log', function (log) {
    logger.log(`${log}`)
  })
  producer.setPollInterval(100)

  // logging all errors
  producer.on('event.error', function (err) {
    producerDone(err)
  })

  // Wait for the ready event before producing
  producer.on('ready', function (arg) {
    logger.info(`ready!`)
    const send = () => {
      const batch = getMessageBatch()
      if (_.isEmpty(batch)) {
        producerDone()
        return
      }
      const sendAfter = _.after(_.size(batch), () => {
        process.nextTick(() => { send() })
      })
      _.forEach(batch, (message) => {
        if (ended) {
          return
        }
        const result = producer.produce(topicName, message.partition, message.value, message.key)
        if (result !== true) {
          logger.warn(`produce did not return true, got ${result}`)
        }
        sentMessage(message)
        sendAfter()
      })
    }
    send()
  })

  producer.on('disconnected', function (arg) {
    producerDone(new Error('Producer Disconnected'))
  })

  // starting the producer
  producer.connect()
}

module.exports = produce
