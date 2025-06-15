const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');

const app = express();
const port = 3000;

const brokerUrl = process.env.KAFKA_BROKER_URL || 'kafka-0:9092';

const kafka = new Kafka({
  clientId: 'client-1',
  brokers: [`${brokerUrl}`]
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner
});

const runProducer = async (message) => {
  await producer.connect();
  console.log('connect to', brokerUrl);
  await producer.send(message);
  await producer.disconnect();
};

app.use(express.json());

app.post('/publish', async (req, res) => {
  try {
    const { topic, value } = req.body;

    if (!topic || !value) {
      return res.status(400).json({ error: '토픽 혹은 메세지 내용이 없습니다.' });
    }

    const message = {
      topic: topic,
      messages: [{ value: value }]
    };

    await runProducer(message);

    console.log('Sent message:', message);
    res.status(200).json({ message: '메세지가 성공적으로 전송되었습니다.' });
  } catch (error) {
    console.error('메세지 전송 중 오류가 발생했습니다:', error);
    res.status(500).json({ error: 'Internal server error.' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
