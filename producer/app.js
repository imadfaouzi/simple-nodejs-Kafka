const express = require('express');
const bodyParser = require('body-parser');
const kafka = require('kafka-node');

const app = express();

app.use(bodyParser.json());

const kafkaServer = process.env.KAFKA_SERVER;
const client = new kafka.KafkaClient({ kafkaHost: kafkaServer });
const producer = new kafka.Producer(client);

producer.on('ready', function() {
  console.log('Producer is ready');
});

producer.on('error', function(err) {
  console.error('Producer error:', err);
});

app.post('/produce', (req, res) => {
  const { topic, message } = req.body;

  if (!topic || !message) {
    return res.status(400).json({ error: 'Invalid request format. Please provide both "topic" and "message".' });
  }

  const payloads = [
    {
      topic,
      messages: message
    }
  ];

  producer.send(payloads, function(err, data) {
    if (err) {
      console.error('Error producing message:', err);
      return res.status(500).json({ error: 'Failed to produce message.' });
    } else {
      console.log('Message sent:', data);
      return res.status(200).json({ success: true });
    }
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Producer server is running on port ${PORT}`);
});
