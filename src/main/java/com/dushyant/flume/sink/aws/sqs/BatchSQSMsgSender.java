/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dushyant.flume.sink.aws.sqs;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch writer implementation for {@link SQSMsgSender}. This writer supports batch messages and writes the specified
 * number of messages to SQS in one batch. The writer always uses
 * <p>
 * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html">
 * SendMessageBatch </a> AWS SQS API even when <i>batchSize = 1</i>.
 *
 * @author dpandya
 */
// PMD rule ShortVariable throws errors for any variable name of 2 or less characters. The variable name "id" is
// perfectly readable variable so suppressing PMD rule ShortVariable
@SuppressWarnings("PMD.ShortVariable")
public class BatchSQSMsgSender implements SQSMsgSender {
    private static final Logger LOG = LoggerFactory.getLogger(BatchSQSMsgSender.class);
    private final long maxMessageSize;
    private int batchSize;
    private String sqsUrl = null;
    private String region = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    private AmazonSQS amazonSQS = null;

    public BatchSQSMsgSender(String sqsUrl, String region, String awsAccessKey, String awsSecretKey, int batchSize,
        long maxMessageSize) {
        this.sqsUrl = sqsUrl;
        this.region = region;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;

        AmazonSQS sqs = null;
        if (StringUtils.isBlank(awsAccessKey) || StringUtils.isBlank(awsSecretKey)) {
            LOG.warn("Either awsAccessKey or awsSecretKey not specified. Will use DefaultAWSCredentialsProviderChain " +
                "to look for AWS credentials.");
            sqs = new AmazonSQSClient();
        }
        else {
            sqs = new AmazonSQSClient(new BasicAWSCredentials(this.awsAccessKey, this.awsSecretKey));
        }

        Region sqsRegion = Region.getRegion(Regions.fromName(this.region));
        sqs.setRegion(sqsRegion);

        this.batchSize = batchSize;
        this.maxMessageSize = maxMessageSize;

        this.amazonSQS = sqs;
    }

    @Override
    public int send(Channel channel) throws EventDeliveryException {
        int eventProcessedCounter = 0;
        // Create batch request
        List<SendMessageBatchRequest> batchRequests = createBatches(channel);

        for (SendMessageBatchRequest batchRequest : batchRequests) {
            // Send batch request
            SendMessageBatchResult result = null;
            try {
                result = this.amazonSQS.sendMessageBatch(batchRequest);
            }
            catch (AmazonServiceException ase) {
                // Throw request reached to SQS but the whole batch was rejected for some reason. Let the whole batch
                // be treated as "failed". Flume will retry the while batch
                throw new EventDeliveryException("Failure sending batch message request to Amazon SQS, " +
                    "the request made it to SQS but was rejected for some reason.", ase);
            }
            catch (AmazonClientException ace) {
                throw new EventDeliveryException("Failure sending batch message request to Amazon SQS.", ace);
            }

            // Handle the result of the SQS batch request i.e., log errors, or fail the whole batch by throwing
            // EventDeliveryException in case of errors etc.
            handleResult(batchRequest, result);

            // The code reached here means there is nothing to rollback in this transaction. So increment the
            // eventProcessedCounter by the number of successfully sent messages.
            eventProcessedCounter += result.getSuccessful().size();
        }
        return eventProcessedCounter;
    }

    /**
     * Handles SQS send message batch result and throws EventDeliveryException to cause the flume transaction to fail
     * and let flume retry the whole batch in case all the messages in the batch failed to be delivered to SQS.
     * Currently, this method does just logs errors and skips the messages in case some messages from the batched failed
     * to be delivered but some succeeded (i.e., partial batch failure).
     * <p>
     * TODO: Add retry logic instead letting flume drop the failed messages in case of partial batch failure
     *
     * @param batchRequest The SQS SendMessageBatchRequest
     * @param batchResult The SQS SendMessageBatchResult
     *
     * @throws EventDeliveryException In case all the messages in the batch failed to be delivered to SQS
     */
    protected void handleResult(SendMessageBatchRequest batchRequest, SendMessageBatchResult batchResult)
        throws EventDeliveryException {

        List<SendMessageBatchRequestEntry> batchRequestEntries = batchRequest.getEntries();
        List<BatchResultErrorEntry> errors = batchResult.getFailed();

        int attemptedCount = batchRequestEntries == null ? 0 : batchRequestEntries.size();
        int errorCount = errors == null ? 0 : errors.size();

        if (errorCount > 0) {
            String errorMessage = buildErrorMessage(batchRequestEntries, errors);

            if (attemptedCount == errorCount) {
                // if it was a non-empty batch and if all the messages in the batch have errors then fail the whole
                // batch and let flume rollback the transaction and retry it
                // Just throw the EventDeliveryException. This will eventually cause the channel's transaction to
                // rollback.
                throw new EventDeliveryException(errorMessage);
            }
            else {
                // TODO: Add retry logic instead letting flume drop the failed messages in case of partial batch failure

                // Just log the error message and let flume drop failed messages in case of partial batch failures
                LOG.error(errorMessage);
            }
        }
    }

    private String buildErrorMessage(List<SendMessageBatchRequestEntry> batchRequestEntries,
        List<BatchResultErrorEntry> errors) {
        StringBuilder errorMessage = new StringBuilder();
        int count = 0;
        for (BatchResultErrorEntry error : errors) {
            if (count > 0) {
                errorMessage.append(",");
            }
            SendMessageBatchRequestEntry failedRequestEventEntry =
                findRequestEventEntryById(batchRequestEntries, error.getId());
            String messageBody = failedRequestEventEntry == null ? null : failedRequestEventEntry.getMessageBody();
            errorMessage.append("[" + error.toString() + ",{messageBody:" + "\"" + messageBody + "\"}]");
            count++;
        }
        return errorMessage.toString();
    }


    private SendMessageBatchRequestEntry findRequestEventEntryById(List<SendMessageBatchRequestEntry> entries,
        String id) {
        SendMessageBatchRequestEntry foundEntry = null;
        if (entries != null) {
            for (SendMessageBatchRequestEntry entry : entries) {
                if (entry.getId().equals(id)) {
                    foundEntry = entry;
                    break;
                }
            }
        }
        return foundEntry;
    }


    /**
     * The method creates batch of requests based on the configured "batchSize" to send to SQS.
     * <p>
     * When the combined payload size of the messages in the batch increases beyond the configured max allowed limit
     * then the method rolls the remaining messages into another batch.
     * <p>
     * For example, let's say that the <i>batchSize</i> is 10 and <i>maxMessageSize</i> is 256Kb and after creating a
     * batch with 7 messages the 256KB size limit is reached, in that case the method will split the remaining 3(i.e. 10
     * - 7) messages into its own batch request.
     * <p>
     * The returned list from this method will usually contain only 1 batch request (with all the messages part of that
     * batch request) when the total message size in the batch is within the allowed limit.
     *
     * @param channel Flume channel to take the messages from
     *
     * @return A list of {@link SendMessageBatchRequest} objects.
     *
     * @throws EventDeliveryException In case the message to be sent to SQS cannot be encoded in UTF-8 format
     */
    protected List<SendMessageBatchRequest> createBatches(Channel channel) throws EventDeliveryException {
        List<SendMessageBatchRequest> batchRequests = new ArrayList<SendMessageBatchRequest>();
        // Create a batch request
        SendMessageBatchRequest batchRequest = new SendMessageBatchRequest(sqsUrl);
        Collection<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
        long numberOfBytesInBatch = 0;
        for (int i = 0; i < batchSize; ++i) {
            // Take event from the channel and add corresponding message to the batch
            Event event = channel.take();
            byte[] msgBytes = (event == null) ? null : event.getBody();
            if (msgBytes == null || msgBytes.length == 0) {
                // Channel returned null or empty event. Just break. Create batch with whatever events we have got so
                // far. This can happen when the channel is empty or is receiving events but with empty body (For
                // example, exec tail source may send empty events).
                break;
            }

            // Using message number as Id. This id is used for identifying the message entries within the batch.
            // It needs to be unique within a batch.
            String id = String.valueOf(i + 1);

            numberOfBytesInBatch += msgBytes.length;

            if (numberOfBytesInBatch > maxMessageSize) {
                // Max size per batch reached. Split into another batch.

                // Add entries collected so far into the current batch
                batchRequest.setEntries(entries);
                // Add the current batch into the list of batches to return
                batchRequests.add(batchRequest);

                // reset byte counter
                numberOfBytesInBatch = 0;

                // create new batch request
                batchRequest = new SendMessageBatchRequest(sqsUrl);
                // reset entries for the new batch
                entries = new ArrayList<SendMessageBatchRequestEntry>();
            }
            SendMessageBatchRequestEntry requestEntry = null;
            try {
                requestEntry = new SendMessageBatchRequestEntry(id, new String(msgBytes, "UTF-8").trim());
            }
            catch (UnsupportedEncodingException e) {
                throw new EventDeliveryException("Character set UTF-8 not supported.", e);
            }
            entries.add(requestEntry);
        }

        // The last batch request may not have been populated yet. Populate if there are any entries to be populated.
        if ((batchRequest.getEntries() == null || batchRequest.getEntries().size() == 0) && entries.size() > 0) {
            batchRequest.setEntries(entries);
            batchRequests.add(batchRequest);
        }
        return batchRequests;
    }

    public void setAmazonSQS(AmazonSQS amazonSQS) {
        this.amazonSQS = amazonSQS;
    }
}
