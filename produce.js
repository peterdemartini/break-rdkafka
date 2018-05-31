const Kafka = require('node-rdkafka')
const _ = require('lodash')
const uuidv4 = require('uuid/v4')
const signale = require('signale')

function produce ({ key, topicName, sentMessage, startBatch, kafkaBrokers }, callback) {
  const logger = signale.scope(key)
  if (!topicName) {
    logger.error(`requires a topicName`)
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
    'debug': 'broker,topic',
    'metadata.broker.list': kafkaBrokers
  })

  // logging debug messages, if debug is enabled
  producer.on('event.log', function (log) {
    if (/(fail|error|warn|issue|disconnect|problem)/gi.test(log.message)) {
      logger.debug(log.message)
    }
  })
  producer.setPollInterval(100)

  // logging all errors
  producer.on('event.error', function (err) {
    logger.error(err)
  })

  // Wait for the ready event before producing
  producer.on('ready', function (arg) {
    logger.info(`ready!`)
    const sendMessages = () => {
      startBatch((err, messages) => {
        if (err) {
          producerDone(err)
          return
        }
        if (_.isEmpty(messages)) {
          producerDone()
          return
        }
        const sendAfter = _.after(_.size(messages), () => {
          sendMessages()
        })
        _.forEach(messages, (message) => {
          if (ended) {
            return
          }
          const value = Buffer.from(uuidv4())
          const result = producer.produce(topicName, message.partition, value, message.key)
          if (result !== true) {
            logger.warn(`produce did not return true, got ${result}`)
          }
          sentMessage(message)
          sendAfter()
        })
      })
    }
    sendMessages()
  })

  producer.on('disconnected', function (arg) {
    producerDone(new Error('Producer Disconnected'))
  })

  // starting the producer
  producer.connect()
}

module.exports = produce
