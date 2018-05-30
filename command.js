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
  const client = new kafka.Client('localhost:2181', 'break-rdkafka', {
    sessionTimeout: 5000,
    spinDelay: 500,
    retries: 0
  })
  client.zk.createTopic(topicName, numPartitions, 1, {}, (err) => {
    if (err) {
      signale.fatal(err)
      return
    }
    const totalMessages = 100000
    const numProducers = 1
    const numConsumers = 4
    _.times(numProducers, (i) => {
      const maxMessages = totalMessages / numProducers
      produce({
        id: `produce-${i}`,
        topicName,
        numPartitions,
        maxMessages,
        kafkaBrokers
      })
    })
    _.times(numConsumers, (i) => {
      const maxMessages = totalMessages / numConsumers
      consume({
        id: `consume-${i}`,
        topicName,
        numPartitions,
        maxMessages,
        kafkaBrokers
      })
    })
  })
}

run()
