const express = require("express");
const kafka = require("../../shared/kafka");

const app = express();

const consumer = kafka.consumer({ groupId: "log-service-group" });

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: "machine-status", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      console.log("Log Service Received:", data);
    },
  });
}

startKafka();

app.get("/", (req, res) => {
  res.send("Log Service Running");
});

app.listen(4001, () => console.log("Log service on 4001"));