const kafka = require('kafka-node');

const kafkaServer = process.env.KAFKA_SERVER;
const client = new kafka.KafkaClient({ kafkaHost: kafkaServer });
const consumer = new kafka.Consumer(client, [{ topic: 'test', partition: 0 }]);

consumer.on('message', function(message) {
  console.log('Received message:', message.value);
});

consumer.on('error', function(err) {
  console.error('Consumer error:', err);
});
