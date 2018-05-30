const produce = require('./produce')
const uuidv4 = require('uuid/v4')
const key = process.env.BREAK_KAFKA_KEY
const topicName = process.env.BREAK_KAFKA_TOPIC_NAME
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS

const sentMessage = (msg) => {
  process.send({ fn: 'sentMessage', msg })
}

const startBatch = (callback) => {
  const requestId = uuidv4()
  process.send({ fn: 'getMessageBatch', requestId })
  const onMessage = ({ fn, messages, responseId }) => {
    if (fn !== 'recieveMessageBatch') {
      return
    }
    if (requestId !== responseId) {
      return
    }
    process.removeListener('message', onMessage)
    callback(null, messages)
  }
  process.on('message', onMessage)
}

process.on('message', ({ fn, msg }) => {
  if (fn === 'sentMessage') {
    sentMessage(msg)
  }
})

produce({ key, topicName, sentMessage, startBatch, kafkaBrokers }, (err) => {
  if (err) {
    throw err
  }
  process.exit()
})
