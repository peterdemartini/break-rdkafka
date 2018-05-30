const _ = require('lodash')
const { kafka } = require('kafka-tools')
const uuidv4 = require('uuid/v4')
const signale = require('signale')
const consume = require('./consume')
const produce = require('./produce')

function run () {
  const topicName = uuidv4()
  const numPartitions = 10
  const kafkaBrokers = 'localhost:9092'
  const client = new kafka.Client('localhost:2181', _.uniqueId('break-kafka-'), {
    sessionTimeout: 5000,
    spinDelay: 500,
    retries: 0
  })
  client.zk.createTopic(topicName, numPartitions, 1, {}, (err) => {
    if (err) {
      signale.fatal(err)
      return
    }
    const totalMessages = 1000000
    const numProducers = 1
    const numConsumers = _.ceil(numPartitions / 4)
    const finalOffsets = {}
    let totalProcessed = 0
    const timeSpacing = 2000
    const exitAfter = _.after(numConsumers, () => {
      signale.success(`processed ${totalProcessed}`)
      console.dir(finalOffsets)
      process.exit(0)
    })
    const shouldFinish = () => {
      ++totalProcessed
      if (totalProcessed === totalMessages) {
        return true
      }
      if (totalProcessed > totalMessages) {
        signale.error('processed more messages than produced')
      }
      return false
    }
    const setupProducer = _.after(numConsumers, () => {
      _.times(numProducers, (i) => {
        setTimeout(() => {
          const key = `produce-${i}`
          const maxMessages = totalMessages / numProducers
          signale.time(key)
          produce({
            logger: signale.scope(key),
            topicName,
            numPartitions,
            maxMessages,
            kafkaBrokers
          }, (err) => {
            signale.timeEnd(key)
            if (err) {
              signale.error(err)
              process.exit(1)
            }
            signale.success(`${key} done!`)
          })
        }, (i + 1) * timeSpacing)
      })
    })
    _.times(numConsumers, (i) => {
      setTimeout(() => {
        const key = `consume-${i + 1}`
        signale.time(key)
        consume({
          logger: signale.scope(key),
          topicName,
          shouldFinish,
          kafkaBrokers
        }, (err, offsets) => {
          _.set(finalOffsets, key, offsets)
          signale.timeEnd(key)
          if (err) {
            signale.error(err)
            process.exit(1)
          }
          signale.success(`${key} done!`)
          exitAfter()
        })
        setupProducer()
      }, i * timeSpacing)
    })
    signale.info(`initializing...
      topic: ${topicName}
      numPartitions: ${numPartitions}
      totalMessages: ${totalMessages}
      consumers: ${numConsumers}
      producers: ${numProducers}
    `)
  })
}

run()
