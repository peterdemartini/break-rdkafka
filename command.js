const _ = require('lodash')
const { kafka } = require('kafka-tools')
const uuidv4 = require('uuid/v4')
const signale = require('signale')
const { fork } = require('child_process')

function run () {
  const topicName = uuidv4()
  const numPartitions = 9
  const messagesPerPartition = 100000
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
    const messages = _.times(messagesPerPartition, () => {
      return _.times(numPartitions, (partition) => ({
        partition,
        key: uuidv4()
      }))
    })
    const totalMessages = _.size(messages)
    const numProducers = 4
    const numConsumers = Math.round(numPartitions / 3)
    const finalOffsets = {}
    const processed = []
    const sent = []
    const children = {}
    const finish = () => {
      if (_.isEmpty(children)) {
        signale.success(`Maybe done?`)
        console.dir(finalOffsets)
        process.exit(0)
      }
      if (_.isEmpty(messages)) {
        killAll(() => {
          signale.success(`DONE!`)
          console.dir(finalOffsets)
          process.exit(0)
        })
      }
    }
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
    const killAll = (callback) => {
      if (_.isEmpty(children)) {
        callback()
        return
      }
      signale.warn('Killing all remaining children')
      _.forEach(_.values(children), (child) => {
        child.kill('SIGTERM')
      })
      setTimeout(() => {
        callback()
      }, 5 * 1000)
    }
    const processedMessage = ({ key }) => {
      const exists = _.find(processed, key)
      if (exists) {
        signale.warn(`processed ${key} more than once`)
      }
      processed.push(key)
      if (_.size(processed) === totalMessages) {
        signale.info(`processed all of the messages`)
        _.forEach(children, (child, key) => {
          signale.info(`sending shouldFinish to ${key}`)
          child.send({ fn: 'shouldFinish' })
        })
      }
      if (_.size(processed) > totalMessages) {
        signale.warn(`processed more messages than it should have`)
      }
    }
    _.times(numProducers, (i) => {
      const key = `produce-${i}`
      signale.time(key)
      const child = fork(`${__dirname}/produce-worker.js`, [], {
        env: {
          BREAK_KAFKA_KEY: key,
          BREAK_KAFKA_TOPIC_NAME: topicName,
          BREAK_KAFKA_BROKERS: kafkaBrokers,
          FORCE_COLOR: '1'
        },
        stdio: 'inherit'
      })
      child.on('close', (code) => {
        delete children[key]
        finish()
        signale.timeEnd(key)
        if (err) {
          signale.error(err)
          killAll()
          return
        }
        signale.success(`${key} done!`)
      })
      child.on('error', (err) => {
        signale.error(err)
      })
      child.on('message', (data) => {
        if (data.fn === 'sentMessage') {
          sentMessage(data.msg)
        }
        if (data.fn === 'getMessageBatch') {
          const batch = messages.splice(0, 100)
          const responseId = data.requestId
          child.send({ fn: 'recieveMessageBatch', messages: batch, responseId })
        }
      })
      children[key] = child
    })
    _.times(numConsumers, (i) => {
      const key = `consume-${i + 1}`
      signale.time(key)
      const child = fork(`${__dirname}/consume-worker.js`, [], {
        env: {
          BREAK_KAFKA_KEY: key,
          BREAK_KAFKA_TOPIC_NAME: topicName,
          BREAK_KAFKA_BROKERS: kafkaBrokers,
          FORCE_COLOR: '1'
        },
        stdio: 'inherit'
      })
      child.on('close', (code) => {
        delete children[key]
        finish()
        signale.timeEnd(key)
        if (err) {
          signale.error(err)
          killAll()
          return
        }
        signale.success(`${key} done!`)
      })
      child.on('error', (err) => {
        signale.error(err)
      })
      child.on('message', (data) => {
        if (data.fn === 'processedMessage') {
          processedMessage(data.msg)
        }
        if (data.fn === 'updateOffsets') {
          finalOffsets[key] = data.offsets
        }
      })
      children[key] = child
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
