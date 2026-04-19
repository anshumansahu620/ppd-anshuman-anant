const express = require("express");
const kafka = require("../../shared/kafka");

const app = express();

// In-memory store (MVP only)
const machineState = {};

const consumer = kafka.consumer({ groupId: "state-service-group" });

async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: "machine-status", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());

      const { machine_id, status, timestamp } = data;

      machineState[machine_id] = {
        status,
        lastUpdated: timestamp,
      };

      console.log("Updated State:", machine_id, machineState[machine_id]);
    },
  });
}

startKafka();

app.get("/machines", (req, res) => {
  res.json(machineState);
});

app.get("/machine/:id", (req, res) => {
  const machine = machineState[req.params.id];

  if (!machine) {
    return res.status(404).json({ error: "Machine not found" });
  }

  res.json(machine);
});

app.listen(4002, () => console.log("State service on 4002"));