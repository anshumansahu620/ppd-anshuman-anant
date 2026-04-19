const kafka = require("../shared/kafka");

const producer = kafka.producer();

async function run() {
  await producer.connect();

  setInterval(async () => {
    const event = {
      machine_id: "M1",
      status: "running",
      timestamp: new Date().toISOString(),
    };

    await producer.send({
      topic: "machine-status",
      messages: [{ value: JSON.stringify(event) }],
    });

    console.log("Sent:", event);
  }, 5000);
}

run();