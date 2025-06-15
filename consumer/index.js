const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
const port = 3000;

const brokerUrl = process.env.KAFKA_BROKER_URL || 'kafka-0:9092';
console.log('connect to', brokerUrl);

const kafka = new Kafka({
  clientId: 'client-2',
  brokers: [`${brokerUrl}`]
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const runConsumer = async (res) => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'cloudtype', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic: topic.toString(),
        partition: partition.toString(),
        value: message.value.toString(),
      });

    },
  });
};

runConsumer().catch(console.error);

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});