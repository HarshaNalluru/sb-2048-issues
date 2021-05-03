// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import {
  delay,
  ProcessErrorArgs,
  ServiceBusAdministrationClient,
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from "@azure/service-bus";
// Load the .env file if it exists
import * as dotenv from "dotenv";
dotenv.config();

async function checkWithTimeout(
  predicate: () => boolean | Promise<boolean>,
  delayBetweenRetriesInMilliseconds: number = 1000,
  maxWaitTimeInMilliseconds: number = 10000
): Promise<boolean> {
  const maxTime = Date.now() + maxWaitTimeInMilliseconds;
  while (Date.now() < maxTime) {
    if (await predicate()) return true;
    await delay(delayBetweenRetriesInMilliseconds);
  }
  return false;
}

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

  const sbClient = new ServiceBusClient(connectionString);
  const sender = sbClient.createSender(queueName);
  const receiver = sbClient.createReceiver(queueName, {
    receiveMode: "receiveAndDelete",
  });

  await sendMessages(sender, numberOfMessagesToSend);
  await receiveMessages(receiver, numberOfMessagesToSend);

  await sbClient.close();
  await serviceBusAdministrationClient.deleteQueue(queueName);
}

main().catch((err) => console.log(err));
