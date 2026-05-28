import { withDurableExecution } from "@aws/durable-execution-sdk-js";
import {
  SQSClient,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  DeleteMessageBatchCommand,
} from "@aws-sdk/client-sqs";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { metricScope, Unit } from "aws-embedded-metrics";
import { performance } from "node:perf_hooks";
/**
 * -------------------------------------------------------
 * AWS Clients
 * -------------------------------------------------------
 */
const sqsClient = new SQSClient({});
const lambdaClient = new LambdaClient({});
/**
 * -------------------------------------------------------
 * Environment Variables
 * -------------------------------------------------------
 */
const DLQ_URL =
  process.env.DLQ_URL ||
  "https://sqs.ap-south-1.amazonaws.com/978902358863/dsqs.fifo";
const SOURCE_QUEUE_URL =
  process.env.SOURCE_QUEUE_URL ||
  "https://sqs.ap-south-1.amazonaws.com/978902358863/nsqs.fifo";
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 10);
const MAX_ITERATIONS = Number(process.env.MAX_ITERATIONS || 150);
const METRIC_NAMESPACE = process.env.METRIC_NAMESPACE || "DLQRedriveService";
const FUNCTION_ALIAS = process.env.FUNCTION_ALIAS || "alias";
/**
 * -------------------------------------------------------
 * Metrics Wrapper
 * -------------------------------------------------------
 */
const trackMetric = metricScope(
  (metrics) =>
    async ({ metricName, step, fn }) => {
      const start = performance.now();
      metrics.setNamespace(METRIC_NAMESPACE);
      metrics.putDimensions({
        Step: step,
      });
      try {
        return await fn(metrics);
      } finally {
        const latency = performance.now() - start;
        metrics.putMetric(metricName, latency, Unit.Milliseconds);
        await metrics.flush();
        console.log(
          JSON.stringify({
            step,
            metricName,
            latencyMs: Number(latency.toFixed(2)),
          }),
        );
      }
    },
);
/**
 * -------------------------------------------------------
 * Main Handler
 * -------------------------------------------------------
 */
export const handler = withDurableExecution(async (event, context) => {
  /**
   * ---------------------------------------------------
   * Stateful Counters
   * ---------------------------------------------------
   */
  let iteration = Number(event.iteration) || 0;
  let processedCount = Number(event.processedCount) || 0;
  let receivedMessagesCount = Number(event.receivedMessagesCount) || 0;
  let pushedSuccessMessagesCount =
    Number(event.pushedSuccessMessagesCount) || 0;
  let pushedFailedMessagesCount = Number(event.pushedFailedMessagesCount) || 0;
  let deletedSuccessMessagesCount =
    Number(event.deletedSuccessMessagesCount) || 0;
  let deletedFailedMessagesCount =
    Number(event.deletedFailedMessagesCount) || 0;
  let nextTriggerNeeded = true;
  /**
   * ---------------------------------------------------
   * Main Processing Loop
   * ---------------------------------------------------
   */
  while (true) {
    iteration++;
    const batchStart = performance.now();
    console.log(`Starting iteration ${iteration}`);
    /**
     * ---------------------------------------------------
     * STEP 1
     * Receive Messages From DLQ
     * ---------------------------------------------------
     */
    const messages = await context.step(
      `Receive Messages From DLQ ${iteration}`,
      async () =>
        trackMetric({
          metricName: "ReceiveMessagesLatency",
          step: "receive",
          fn: async (metrics) => {
            const response = await sqsClient.send(
              new ReceiveMessageCommand({
                QueueUrl: DLQ_URL,
                MaxNumberOfMessages: BATCH_SIZE,
                WaitTimeSeconds: 5,
                VisibilityTimeout: 60,
                AttributeNames: ["All"],
                MessageAttributeNames: ["All"],
              }),
            );
            const receivedMessages = response.Messages || [];
            receivedMessagesCount += receivedMessages.length;
            metrics.putMetric(
              "MessagesReceived",
              receivedMessages.length,
              Unit.Count,
            );
            console.log(`Received ${receivedMessages.length} messages`);
            return receivedMessages;
          },
        }),
    );
    /**
     * ---------------------------------------------------
     * No More Messages
     * ---------------------------------------------------
     */
    if (messages.length === 0) {
      console.log("No messages left in DLQ");
      nextTriggerNeeded = false;
      break;
    }
    /**
     * ---------------------------------------------------
     * STEP 2
     * Push Messages To Source Queue
     * ---------------------------------------------------
     */
    const sendResult = await context.step(
      `Push Messages To Source Queue ${iteration}`,
      async () =>
        trackMetric({
          metricName: "PushMessagesLatency",
          step: "push",
          fn: async (metrics) => {
            const entries = messages.map((message, index) => {
              const entry = {
                Id: message.MessageId || `${index}`,
                MessageBody: message.Body || "",
                MessageAttributes: message.MessageAttributes,
              };
              /**
               * FIFO Support
               */
              if (SOURCE_QUEUE_URL.endsWith(".fifo")) {
                entry.MessageGroupId =
                  message.Attributes?.MessageGroupId || "default-group";
                entry.MessageDeduplicationId = `${message.MessageId}-${Date.now()}`;
              }
              return entry;
            });
            const response = await sqsClient.send(
              new SendMessageBatchCommand({
                QueueUrl: SOURCE_QUEUE_URL,
                Entries: entries,
              }),
            );
            const successful = response.Successful?.length || 0;
            const failed = response.Failed?.length || 0;
            pushedSuccessMessagesCount += successful;
            pushedFailedMessagesCount += failed;
            metrics.putMetric("MessagesPushed", successful, Unit.Count);
            metrics.putMetric("MessagesPushFailed", failed, Unit.Count);
            console.log(`Successfully pushed ${successful} messages`);
            if (failed > 0) {
              console.error(
                "Push failures",
                JSON.stringify(response.Failed, null, 2),
              );
            }
            return response;
          },
        }),
    );
    /**
     * ---------------------------------------------------
     * STEP 3
     * Delete Successfully Sent Messages
     * ---------------------------------------------------
     */
    const deleteResult = await context.step(
      `Delete Messages From DLQ ${iteration}`,
      async () =>
        trackMetric({
          metricName: "DeleteMessagesLatency",
          step: "delete",
          fn: async (metrics) => {
            const successfulIds = new Set(
              (sendResult.Successful || []).map((message) => message.Id),
            );
            const deleteEntries = messages
              .filter((message, index) => {
                const id = message.MessageId || `${index}`;
                return successfulIds.has(id);
              })
              .map((message, index) => ({
                Id: message.MessageId || `${index}`,
                ReceiptHandle: message.ReceiptHandle,
              }));
            if (deleteEntries.length === 0) {
              console.log("No messages to delete");
              return;
            }
            const response = await sqsClient.send(
              new DeleteMessageBatchCommand({
                QueueUrl: DLQ_URL,
                Entries: deleteEntries,
              }),
            );
            const successful = response.Successful?.length || 0;
            const failed = response.Failed?.length || 0;
            deletedSuccessMessagesCount += successful;
            deletedFailedMessagesCount += failed;
            processedCount += successful;
            metrics.putMetric("MessagesDeleted", successful, Unit.Count);
            metrics.putMetric("MessagesDeleteFailed", failed, Unit.Count);
            console.log(`Deleted ${successful} messages`);
            if (failed > 0) {
              console.error(
                "Delete failures",
                JSON.stringify(response.Failed, null, 2),
              );
            }
            return response;
          },
        }),
    );
    /**
     * ---------------------------------------------------
     * Batch Metrics
     * ---------------------------------------------------
     */
    const batchLatency = performance.now() - batchStart;
    console.log(`Batch completed in ${batchLatency.toFixed(2)} ms`, {
      deleteResult,
    });
    /**
     * ---------------------------------------------------
     * Prevent Endless Execution
     * ---------------------------------------------------
     */
    await wait(context, iteration);
    if (iteration >= MAX_ITERATIONS) {
      console.log("Iteration limit reached");
      break;
    }
  }
  /**
   * ---------------------------------------------------
   * STEP 4
   * Trigger Next Durable Execution
   * ---------------------------------------------------
   */
  if (nextTriggerNeeded) {
    await context.step(`Trigger Next Execution ${iteration}`, async () =>
      trackMetric({
        metricName: "TriggerNextExecutionLatency",
        step: "trigger",
        fn: async (metrics) => {
          const command = new InvokeCommand({
            FunctionName: `${process.env.AWS_LAMBDA_FUNCTION_NAME}:${FUNCTION_ALIAS}`,
            InvocationType: "Event",
            Payload: JSON.stringify({
              iteration: 0,
              processedCount,
              receivedMessagesCount,
              pushedSuccessMessagesCount,
              pushedFailedMessagesCount,
              deletedSuccessMessagesCount,
              deletedFailedMessagesCount,
            }),
          });
          await lambdaClient.send(command);
          metrics.putMetric("DurableExecutionTriggered", 1, Unit.Count);
          console.log("Triggered next durable execution");
        },
      }),
    );
  }
  /**
   * ---------------------------------------------------
   * Final Response
   * ---------------------------------------------------
   */
  return {
    status: "completed",
    iteration,
    processedCount,
    receivedMessagesCount,
    pushedSuccessMessagesCount,
    pushedFailedMessagesCount,
    deletedSuccessMessagesCount,
    deletedFailedMessagesCount,
  };
});

async function wait(context, iteration) {
  const timeLeft = context.lambdaContext.getRemainingTimeInMillis();
  console.log(`timeLeft`, timeLeft);
  const shouldWait = await context.step("calculate-wait", async () => {
    return context.lambdaContext.getRemainingTimeInMillis() < 5000;
  });

  if (shouldWait) {
    await context.wait({ seconds: 3 });
  }
  return false;
}
