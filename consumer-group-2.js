const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const ip = require('ip');

const app = express();

const host = process.env.HOST_IP || ip.address();

const port = process.argv[2] || 3001;

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:9092`],
  clientId: 'example-consumer',
});

const topic = 'topic-test'
const consumer = kafka.consumer({ groupId: 'test-group-2' });

const receivedMessages = {};

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      const { value, key } = message;
      console.log('Received message - ' + message.value);
      receivedMessages[key] = {
        prefix,
        value: value.toString()
      };
    },
  })
};

app.get('/status/:messageId', async (req, res)=> {
  res.json(receivedMessages[req.params.messageId] || 'Not Found');
});

run().catch(e => console.error(`[example/consumer] ${e.message}`, e));

app.listen(port, () => console.log(`Example app listening on port ${port}!`))