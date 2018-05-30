const Kafka = require('node-rdkafka')
const _ = require('lodash')

function produce ({ logger, topicName, numPartitions, maxMessages, kafkaBrokers }, callback) {
  if (!topicName) {
    logger.error(`requires a topicName`)
    process.exit(1)
  }

  if (!numPartitions) {
    logger.error(`requires a numPartitions`)
    process.exit(1)
  }

  if (!maxMessages) {
    logger.error(`requires a maxMessages`)
    process.exit(1)
  }
  logger.info(`initializing...`)

  let pollLoop
  let ended = false
  const producerDone = _.once((err) => {
    ended = true
    clearInterval(pollLoop)
    if (producer.isConnected()) {
      producer.disconnect()
    }
    if (err) {
      callback(err)
      return
    }
    logger.success(`DONE! ${maxMessages}`)
    callback()
  })

  const producer = new Kafka.Producer({
    'client.id': _.uniqueId('break-kafka-'),
    // 'debug' : 'all',
    'metadata.broker.list': kafkaBrokers,
    'dr_cb': true // delivery report callback
  })

  // logging debug messages, if debug is enabled
  producer.on('event.log', function (log) {
    logger.log(`${log}`)
  })

  // logging all errors
  producer.on('event.error', function (err) {
    producerDone(err)
  })

  const endAfter = _.after(maxMessages, producerDone)

  producer.on('delivery-report', function (err, report) {
    if (err) {
      producerDone(err)
      return
    }
    endAfter()
  })

  // Wait for the ready event before producing
  producer.on('ready', function (arg) {
    logger.info(`ready!`)
    _.times(maxMessages / numPartitions, (i) => {
      setTimeout(() => {
        _.times(numPartitions, (p) => {
          if (ended) {
            return
          }
          const value = Buffer.from('value-' + i + '-partition-' + p)
          const key = 'key-' + i
          producer.produce(topicName, p, value, key)
        })
      }, i)
    })
    clearInterval(pollLoop)
    pollLoop = setInterval(function () {
      producer.poll()
    }, 1000)
  })

  producer.on('disconnected', function (arg) {
    producerDone(new Error('Producer Disconnected'))
  })

  // starting the producer
  producer.connect()
}

module.exports = produce
