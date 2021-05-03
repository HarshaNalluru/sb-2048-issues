// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

import {
  delay,
  ProcessErrorArgs,
  ServiceBusAdministrationClient,
  ServiceBusClient,
  ServiceBusReceivedMessage,
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
        timeToLive: 10000,
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

async function main() {
  // subscribe - peekLock
  // - send
  // - receive 2048 messages
  // - do not settle them
  // - wait for 10 seconds
  // - make sure no new messages were received
  // - settle one message
  // - wait for 30 seconds
  // - we should receive one new message by now
  // - settle all the messages
  // - rest would have been received
  // - settle all of them
  // - verifyMessageCount

  // Define connection string and related Service Bus entity names here
  const connectionString =
    process.env.SERVICEBUS_CONNECTION_STRING || "<connection string>";
  const queueName = `qqqq-${Math.floor(Math.random() * 9000 + 1000)}`;
  const serviceBusAdministrationClient = new ServiceBusAdministrationClient(
    connectionString
  );
  // Similarly, you can create topics and subscriptions as well.
  const createQueueResponse = await serviceBusAdministrationClient.createQueue(
    queueName,
    { enablePartitioning: true }
  );
  console.log("Created queue with name - ", createQueueResponse.name);
  const numberOfMessagesToSend = 3000;

  const sbClient = new ServiceBusClient(connectionString);
  const sender = sbClient.createSender(queueName);
  const receiver = sbClient.createReceiver(queueName);

  await sendMessages(sender, numberOfMessagesToSend);
  const receivedBodies: any[] = [];
  let numberOfMessagesReceived = 0;
  let reachedBufferCapacity = false;
  const firstBatch: ServiceBusReceivedMessage[] = [];
  const secondBatch: ServiceBusReceivedMessage[] = [];
  receiver.subscribe(
    {
      async processMessage(msg: ServiceBusReceivedMessage) {
        numberOfMessagesReceived++;
        receivedBodies.push(msg.body);
        if (!reachedBufferCapacity) {
          firstBatch.push(msg);
        } else {
          secondBatch.push(msg);
        }
      },
      async processError(_args: ProcessErrorArgs) {},
    },
    {
      maxConcurrentCalls: 2000,
      autoCompleteMessages: false,
    }
  );
  await checkWithTimeout(() => numberOfMessagesReceived === 2047, 1000, 100000);
  console.log(
    `numberOfMessagesReceived (before wait) = ${numberOfMessagesReceived}`
  );
  await delay(2000);
  console.log(
    `numberOfMessagesReceived (after wait) = ${numberOfMessagesReceived}`
  );

  await receiver.completeMessage(firstBatch.shift()!); // settle the first message

  console.log(`completed one message and waiting for 3 seconds`);
  await delay(3000);
  console.log(
    `numberOfMessagesReceived (after completing message) = ${numberOfMessagesReceived}`
  );
  await Promise.all(firstBatch.map((msg) => receiver.completeMessage(msg)));
  console.log(`completed all first batch and waiting for 15 seconds`);
  await delay(15000);
  console.log(
    `numberOfMessagesReceived (after completing all first batch) = ${numberOfMessagesReceived}`
  );

  await sbClient.close();

  await serviceBusAdministrationClient.deleteQueue(queueName);
}

main().catch((err) => console.log(err));
