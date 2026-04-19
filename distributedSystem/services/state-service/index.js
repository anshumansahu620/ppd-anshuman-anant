const express = require("express");
const kafka = require("../../shared/kafka");
const pool = require("./db");

const app = express();

const machineState = {};

// ✅ Kafka consumer
const consumer = kafka.consumer({
  groupId: "state-service-group",
  retry: {
    initialRetryTime: 300,
    retries: 5,
  },
});

// 🔹 Start Kafka consumer
async function startKafka() {
  await consumer.connect();
  await consumer.subscribe({ topic: "machine-status", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());

        const { machine_id, status, timestamp } = data;

        // ✅ Safe timestamp parsing
        const parsedTime = Date.parse(timestamp);
        const lastUpdated = isNaN(parsedTime) ? Date.now() : parsedTime;

        // ✅ Update in-memory state
        machineState[machine_id] = {
          status,
          lastUpdated,
        };

        console.log("Updated:", machine_id, machineState[machine_id]);

        // ✅ Save to DB (UPSERT)
        await pool.query(
          `
          INSERT INTO machine_state (machine_id, status, last_updated)
          VALUES ($1, $2, $3)
          ON CONFLICT (machine_id)
          DO UPDATE SET
            status = EXCLUDED.status,
            last_updated = EXCLUDED.last_updated
          `,
          [machine_id, status, lastUpdated]
        );

        console.log("Saved to DB:", machine_id);
      } catch (err) {
        console.error("Error processing message:", err.message);
      }
    },
  });
}

async function loadStateFromDB() {
  try {
    const res = await pool.query("SELECT * FROM machine_state");

    res.rows.forEach((row) => {
      machineState[row.machine_id] = {
        status: row.status,
        lastUpdated: Number(row.last_updated), // 🔴 FIX
      };
    });

    console.log("Loaded state from DB:", machineState);
  } catch (err) {
    console.error("Error loading state from DB:", err.message);
  }
}

async function startService() {
  await loadStateFromDB();
  await startKafka();
}

startService();

// 🔴 Downtime detection
setInterval(() => {
  const now = Date.now();

  Object.keys(machineState).forEach((machineId) => {
    const machine = machineState[machineId];

    if (!machine || !machine.lastUpdated) return;

    const diff = now - machine.lastUpdated;

    if (diff < 0) return;

    if (diff > 10000 && machine.status !== "stopped") {
      machine.status = "stopped";
      console.log(`Machine ${machineId} marked STOPPED`);
    }
  });
}, 5000);

// 🔹 API - All machines
app.get("/machines", (req, res) => {
  const formatted = {};

  Object.keys(machineState).forEach((id) => {
    const machine = machineState[id];

    formatted[id] = {
      status: machine.status,
      lastUpdated: new Date(machine.lastUpdated).toISOString(),
    };
  });

  res.json(formatted);
});

// 🔹 API - Single machine
app.get("/machine/:id", (req, res) => {
  const machine = machineState[req.params.id];

  if (!machine) {
    return res.status(404).json({ error: "Machine not found" });
  }

  res.json({
    status: machine.status,
    lastUpdated: new Date(machine.lastUpdated).toISOString(),
  });
});

// 🔹 Start server
app.listen(4002, () => console.log("State service on 4002"));