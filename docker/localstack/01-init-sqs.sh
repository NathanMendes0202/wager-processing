#!/bin/sh
set -eu

# JSON completo em --attributes: a sintaxe curta quebra na RedrivePolicy (virgulas).
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

awslocal sqs create-queue --queue-name wager-transactions.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"30","RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:wager-transactions-dlq.fifo\",\"maxReceiveCount\":\"5\"}"}'

awslocal sqs create-queue --queue-name wager-events-dlq.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

awslocal sqs create-queue --queue-name wager-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'
