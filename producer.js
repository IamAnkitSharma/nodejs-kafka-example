
const express = require('express');
const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const ip = require('ip');

const app = express();
app.use(express.json());

const host = process.env.HOST_IP || ip.address()

const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`${host}:9092`],
  clientId: 'example-producer',
})

const topic = 'topic-test';
const producer = kafka.producer()

const sendMessage = (key, value) => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{
        key,
        value,
      }],
    });
}

const run = async () => {
  await producer.connect();
};

app.post('/send', async (req, res)=> {
    const messageId = Math.random().toString(36).substring(2,7);
    const data = await sendMessage(messageId, req.body.value || '');
    res.json({ messageId, data });
});

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

app.listen(3000, () => console.log('Example app listening on port 3000!'))