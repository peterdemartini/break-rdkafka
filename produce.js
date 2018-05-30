const Kafka = require('node-rdkafka')
const _ = require('lodash')
const signale = require('signale')

function produce ({ id, topicName, numPartitions, maxMessages, kafkaBrokers }) {
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

  const producer = new Kafka.Producer({
    // 'debug' : 'all',
    'metadata.broker.list': kafkaBrokers,
    'dr_cb': true // delivery report callback
  })

  // logging debug messages, if debug is enabled
  producer.on('event.log', function (log) {
    signale.log(`[${id}] ${log}`)
  })

  // logging all errors
  producer.on('event.error', function (err) {
    signale.error(`[${id}] Error from producer ${err}`)
  })

  let pollLoop
  let ended = false
  const endAfter = _.after(maxMessages, () => {
    signale.success(`[${id}] Done processing ${maxMessages}`)
    ended = true
    signale.timeEnd(`[${id}]`)
    clearInterval(pollLoop)
    producer.disconnect()
    process.exit(1)
  })

  producer.on('delivery-report', function (err, report) {
    if (err) {
      signale.error(`[${id}] devilery report error ${err.toString()}`)
      return
    }
    endAfter()
  })

  // Wait for the ready event before producing
  producer.on('ready', function (arg) {
    signale.time(`[${id}]`)
    signale.info(`[${id}] Starting... topic: ${topicName} maxMessages: ${maxMessages}`)
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
    signale.log(`[${id}] disconnected ${JSON.stringify(arg)}`)
  })

  // starting the producer
  producer.connect()
}

module.exports = produce
