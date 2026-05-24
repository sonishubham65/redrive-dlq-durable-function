# DLQ Redrive Service

A scalable AWS Lambda-based DLQ (Dead Letter Queue) redrive service built using:

- AWS Lambda
- AWS SQS
- AWS Durable Execution SDK
- CloudWatch Embedded Metrics

This service continuously pulls messages from a DLQ and republishes them to the source queue in batches with retry-safe durable execution.

---

## Features

- Durable execution support
- Batch-based DLQ message processing
- FIFO queue support
- CloudWatch metrics integration
- Automatic self-triggering continuation
- Partial failure handling
- Configurable batch size and iteration limits

---

## Architecture

```text
DLQ (SQS)
   |
   v
Lambda Durable Execution
   |
   +--> Receive Messages
   +--> Push To Source Queue
   +--> Delete Successfully Processed Messages
   |
   v
Re-trigger Lambda (if more messages exist)
```

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DLQ_URL` | Dead Letter Queue URL | Required |
| `SOURCE_QUEUE_URL` | Source Queue URL | Required |
| `BATCH_SIZE` | Number of messages per batch | `10` |
| `MAX_ITERATIONS` | Max iterations per execution | `150` |
| `METRIC_NAMESPACE` | CloudWatch metric namespace | `DLQRedriveService` |
| `FUNCTION_ALIAS` | Lambda alias used for self-trigger | `alias` |

---

## How It Works

### Step 1 — Receive Messages

The Lambda pulls messages from the DLQ using:

- Long polling
- Visibility timeout
- Batch receive

### Step 2 — Push Messages

Messages are republished to the source queue.

FIFO queues are automatically supported using:

- `MessageGroupId`
- `MessageDeduplicationId`

### Step 3 — Delete Messages

Only successfully republished messages are deleted from the DLQ.

### Step 4 — Continue Execution

If more messages remain, the Lambda asynchronously invokes itself to continue processing.

---

## Metrics

The service publishes CloudWatch Embedded Metrics for:

- Receive latency
- Push latency
- Delete latency
- Trigger latency
- Messages received
- Messages pushed
- Messages deleted
- Failed operations

---

## Installation

```bash
npm install
```

---

## Build

```bash
npm run build
```

---

## Deploy

Example using AWS SAM:

```bash
sam build
sam deploy
```

---

## Example IAM Permissions

The Lambda requires:

```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "sqs:SendMessage",
    "sqs:GetQueueAttributes"
  ],
  "Resource": "*"
}
```

And permission to invoke itself:

```json
{
  "Effect": "Allow",
  "Action": "lambda:InvokeFunction",
  "Resource": "*"
}
```

---

## FIFO Queue Notes

If the source queue is FIFO:

- `.fifo` suffix is automatically detected
- `MessageGroupId` is preserved
- Deduplication IDs are regenerated

---

## Failure Handling

The service safely handles:

- Partial batch failures
- Push failures
- Delete failures
- Durable retries

Messages are only deleted after successful republishing.

---

## Tech Stack

- Node.js
- AWS Lambda
- AWS SQS
- AWS SDK v3
- AWS Durable Execution SDK
- CloudWatch Embedded Metrics

---

## Future Improvements

- Parallel batch processing
- DLQ depth monitoring
- Adaptive batch sizing
- EventBridge scheduling
- Multi-queue support
- Dashboard integration

---

## License

MIT
