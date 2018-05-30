const consume = require('./consume')
const key = process.env.BREAK_KAFKA_KEY
const topicName = process.env.BREAK_KAFKA_TOPIC_NAME
const kafkaBrokers = process.env.BREAK_KAFKA_BROKERS

let finished = false
const updateOffsets = (offsets) => {
  process.send({ fn: 'updateOffsets', offsets })
}

const processedMessage = (msg) => {
  process.send({ fn: 'sentMessage', msg })
}

const shouldFinish = () => {
  return finished
}

process.on('message', ({ fn }) => {
  if (fn === 'shouldFinish') {
    finished = true
  }
})

consume({ key, topicName, updateOffsets, processedMessage, shouldFinish, kafkaBrokers }, (err) => {
  if (err) {
    throw err
  }
  process.exit()
})
