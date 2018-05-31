const _ = require('lodash')
const { kafka } = require('kafka-tools')
const uuidv4 = require('uuid/v4')
const signale = require('signale')
const { fork } = require('child_process')

function run () {
  const topicName = uuidv4()
  const numPartitions = 10
  const numProducers = 2
  const numConsumers = 5
  const messagesPerPartition = 100000
  const messages = _.flatten(_.times(messagesPerPartition, () => {
    return _.times(numPartitions, (partition) => ({
      partition,
      key: uuidv4()
    }))
  }))
  const totalMessages = _.size(messages)
  const kafkaBrokers = 'localhost:9092,localhost:9093'
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
    const children = {}
    const finalOffsets = {}
    const processed = {}
    const sent = {}
    _.times(numPartitions, (par) => {
      sent[`${par}`] = 0
      processed[`${par}`] = 0
    })
    const exitNow = _.once((err) => {
      signale.info('Exiting in 5 seconds')
      _.delay(() => {
        signale.info('Exiting now...')
        killAll(() => {
          client.zk.deleteTopics([topicName], (dErr) => {
            if (dErr) {
              signale.error(dErr)
            }
            if (err) {
              signale.fatal(err)
              process.exit(1)
            }
            signale.success(`DONE!`)
            console.log('offsets', finalOffsets)
            console.log('processed', processed)
            console.log('sent', sent)
            process.exit(0)
          })
        })
      }, 5000)
    })
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
    const exitIfNeeded = () => {
      if (_.isEmpty(messages)) {
        exitNow()
      }
      if (_.isEmpty(children)) {
        exitNow()
      }
    }
    const shouldFinishChildren = _.once(() => {
      _.forEach(children, (child, key) => {
        signale.info(`sending shouldFinish to ${key}`)
        child.send({ fn: 'shouldFinish' })
      })
    })
    const sentMessage = (input) => {
      sent[`${input.partition}`] += 1
      if (_.sum(_.values(sent)) > totalMessages) {
        signale.warn(`sent more messages than it should have`)
      }
    }
    const processedMessage = (input) => {
      processed[`${input.partition}`] += 1
      if (_.sum(_.values(processed)) === totalMessages) {
        signale.info(`processed all of the messages`)
        shouldFinishChildren()
      }
      if (_.sum(_.values(processed)) > totalMessages) {
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
        if (err) {
          exitNow(new Error(`${key} died with an exit code of ${code}`))
          return
        }
        exitIfNeeded()
        signale.timeEnd(key)
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
          const batch = messages.splice(0, 1000)
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
        if (code > 0) {
          exitNow(new Error(`${key} died with an exit code of ${code}`))
          return
        }
        exitIfNeeded()
        signale.timeEnd(key)
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
