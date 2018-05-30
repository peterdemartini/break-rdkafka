const _ = require('lodash')
const { kafka } = require('kafka-tools')
const uuidv4 = require('uuid/v4')
const signale = require('signale')
const consume = require('./consume')
const produce = require('./produce')

function run () {
  const topicName = uuidv4()
  const numPartitions = 9
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
    const numConsumers = Math.round(numPartitions / 3)
    const finalOffsets = {}
    const processed = []
    const sent = []
    const timeSpacing = 2000
    const exitAfter = _.after(numConsumers, () => {
      signale.success(`processed ${_.size(processed)}`)
      console.dir(finalOffsets)
      process.exit(0)
    })
    const sentMessage = ({ key }) => {
      const exists = _.find(sent, key)
      if (exists) {
        signale.warn(`sent ${key} more than once`)
      }
      sent.push(key)
      if (_.size(sent) > totalMessages) {
        signale.warn(`sent more messages than it should have`)
      }
    }
    const getMessageBatch = () => {
      const remaining = totalMessages - _.size(sent)
      const value = Buffer.from(_.uniqueId('value-'))
      const key = _.uniqueId('key-')
      const count = (remaining >= numPartitions) ? numPartitions : remaining
      return _.times(count, (partition) => {
        return {
          partition,
          value,
          key
        }
      })
    }
    const processedMessage = ({ key }) => {
      const exists = _.find(processed, key)
      if (exists) {
        signale.warn(`processed ${key} more than once`)
      }
      processed.push(key)
      if (_.size(processed) === totalMessages) {
        return true
      }
      if (_.size(processed) > totalMessages) {
        signale.warn(`processed more messages than it should have`)
      }
      return false
    }
    const setupProducer = _.after(numConsumers, () => {
      _.times(numProducers, (i) => {
        setTimeout(() => {
          const key = `produce-${i}`
          signale.time(key)
          produce({
            key,
            topicName,
            numPartitions,
            getMessageBatch,
            sentMessage,
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
          key,
          topicName,
          processedMessage,
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
