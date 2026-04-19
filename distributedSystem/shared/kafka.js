const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "observability-system",
  brokers: ["localhost:9092"],
});

module.exports = kafka;