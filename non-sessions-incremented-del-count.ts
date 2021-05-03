// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import {
  ServiceBusAdministrationClient,
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from "@azure/service-bus";
// Load the .env file if it exists
import * as dotenv from "dotenv";
dotenv.config();

async function sendMessages(
  sender: ServiceBusSender,
  numberOfMessagesToSend: number
) {
  let current = 0;
  const messageBodies = [];
  while (current < numberOfMessagesToSend) {
    const batch = await sender.createMessageBatch();
    let body = `message-${current}`;
    while (
      current < numberOfMessagesToSend &&
      batch.tryAddMessage({
        body,
      })
    ) {
      messageBodies.push(body);
      current++;
      body = `message-${current}`;
    }

    await sender.sendMessages(batch);
  }
  return messageBodies;
}

async function receiveMessages(
  receiver: ServiceBusReceiver,
  numberOfMessagesToReceive: number
) {
  let messages: ServiceBusReceivedMessage[] = [];
  while (messages.length < numberOfMessagesToReceive) {
    messages = messages.concat(
      await receiver.receiveMessages(50, { maxWaitTimeInMs: 1500 })
    );
    console.log(`...Received ${messages.length} messages in total`);
  }
  console.log("Receiving done!");
  return messages;
}

async function main() {
  // Define connection string and related Service Bus entity names here
  const connectionString =
    process.env.SERVICEBUS_CONNECTION_STRING || "<connection string>";
  const queueName = `qqqq-${Math.floor(Math.random() * 9000 + 1000)}`;
  const serviceBusAdministrationClient = new ServiceBusAdministrationClient(
    connectionString
  );

  const createQueueResponse = await serviceBusAdministrationClient.createQueue(
    queueName,
    { enablePartitioning: true }
  );
  console.log("Created queue with name - ", createQueueResponse.name);
  const numberOfMessagesToSend = 2000;

  let sbClient = new ServiceBusClient(connectionString);
  const sender = sbClient.createSender(queueName);
  await sendMessages(sender, numberOfMessagesToSend);

  let receiver = sbClient.createReceiver(queueName);
  await receiveMessages(receiver, numberOfMessagesToSend);
  await sbClient.close();
  console.log("Closed the client");

  sbClient = new ServiceBusClient(connectionString);
  receiver = sbClient.createReceiver(queueName);

  const messages = await receiveMessages(receiver, numberOfMessagesToSend);
  const delCount = new Array(10).fill(0, 0, 10);
  for (const message of messages) {
    if (message.deliveryCount) {
      delCount[message.deliveryCount]++;
    }
  }
  console.log(`delCount[0] => ${delCount[0]}`);
  console.log(`delCount[1] => ${delCount[1]}`);
  console.log(`delCount[2] => ${delCount[2]}`);

  await sbClient.close();
  await serviceBusAdministrationClient.deleteQueue(queueName);
}

main().catch((err) => console.log(err));
