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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.BatchResultErrorEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageBatchResultEntry;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test class for {@link BatchSQSMsgSender}
 *
 * @author dpandya
 */
public class BatchSQSMsgSenderTest {
    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests happy path scenario.
     * <p>
     * <pre>
     * Inputs:
     *  channel = never empty
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *  each message size = 2 Bytes
     *
     * Expected Output:
     *  number of batches = 1
     *  number of messages in batch = 5
     * </pre>
     */
    @Test
    public void testCreateBatches() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);

        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(1, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries);
        Assert.assertEquals(5, msgEntries.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries);
    }

    /**
     * Tests {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests invalid characters not
     * allowed by the SQS. See [http://docs.aws.amazon
     * .com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html]
     * for list of valid characters allowed by SQS.
     * <p>
     * <p>
     * <pre>
     * Inputs:
     *  channel = never empty. with messages containing invalid characters.
     *
     * Expected Output:
     *   The sink messages should not contain invalid characters
     * </pre>
     */
    @Test
    public void testInvalidCharacters() throws Exception {
        // See
        // http://stackoverflow.com/questions/16688523/aws-sqs-valid-characters
        // http://stackoverflow.com/questions/1169754/amazon-sqs-invalid-binary-character-in-message-body
        // https://forums.aws.amazon.com/thread.jspa?messageID=459090
        // http://stackoverflow.com/questions/16329695/invalid-binary-character-when-transmitting-protobuf-net
        // -messages-over-aws-sqs
        byte invalidCharByte = 0x1C;
        String mockMsg = "Test with some invalid chars at the end 0%2F>^F";
        byte[] origPayloadWithInvalidChars = ArrayUtils.add(mockMsg.getBytes(), invalidCharByte);

        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 1,
                origPayloadWithInvalidChars.length);

        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(origPayloadWithInvalidChars);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        assertCorrectPayloadInEntries(new String(origPayloadWithInvalidChars).trim().getBytes(), msgEntries);

        // Make sure that the message being sent by the sink doesn't contain the invalid characters
        for (SendMessageBatchRequestEntry entry : msgEntries) {
            Assert.assertNotNull(entry);
            Assert.assertTrue(ArrayUtils.contains(new String(origPayloadWithInvalidChars).getBytes(), invalidCharByte));
            Assert.assertTrue(!ArrayUtils.contains(entry.getMessageBody().getBytes(), invalidCharByte));
        }
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel is empty.
     * <p>
     * <pre>
     * Inputs:
     *  channel = empty
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 0
     * </pre>
     */
    @Test
    public void testCreateBatchesEmptyChannel() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(null);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(0, batches.size());
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel returns event with empty body.
     * <p>
     * <pre>
     * Inputs:
     *  channel = 1 event with empty body
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 0
     * </pre>
     */
    @Test
    public void testCreateBatchesEventWithEmptyBody() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);

        Channel mockChannel = Mockito.mock(Channel.class);
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn("".getBytes());
        when(mockChannel.take()).thenReturn(mockEvent);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(0, batches.size());
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel is empty after first event.
     * <p>
     * <pre>
     * Inputs:
     *  channel = 1 Event (Empty after first take)
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 1
     *  number of messages in batch = 1
     * </pre>
     */
    @Test
    public void testCreateBatchesEmptyChannelAfterFirstTake() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent).thenReturn(null);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(1, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries);
        Assert.assertEquals(1, msgEntries.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel is empty after the last take for the batch.
     * <p>
     * <pre>
     * Inputs:
     *  channel = 5 Events (Empty after 5th take)
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 1
     *  number of messages in batch = 5
     * </pre>
     */
    @Test
    public void testCreateBatchesEmptyChannelAfterLastTake() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent, mockEvent, mockEvent, mockEvent, mockEvent, null);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(1, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries);
        Assert.assertEquals(5, msgEntries.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries);
    }


    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel is empty in the middle of taking events for the batch
     * <p>
     * <pre>
     * Inputs:
     *  channel = 3 Events (Empty after 3rd take)
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 1
     *  number of messages in batch = 3
     * </pre>
     */
    @Test
    public void testCreateBatchesEmptyChannelInTheMiddle() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent, mockEvent, mockEvent, null);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(1, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries);
        Assert.assertEquals(3, msgEntries.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * channel is not empty but contains events with empty body in the middle of taking events for the batch
     * <p>
     * <pre>
     * Inputs:
     *  channel = 4 Events (3 Events with Body and 4th Event empty)
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *
     * Expected Output:
     *  number of batches = 1
     *  number of messages in batch = 3
     * </pre>
     */
    @Test
    public void testCreateBatchesEmptyEventInTheMiddle() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        byte[] mockMsgPayload = {'A', 'b'};
        byte[] mockEmptyMsgPayload = {};
        Event mockEvent = Mockito.mock(Event.class);
        Event mockEmptyEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);
        when(mockEmptyEvent.getBody()).thenReturn(mockEmptyMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent, mockEvent, mockEvent, mockEmptyEvent);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(1, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries);
        Assert.assertEquals(3, msgEntries.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * specified <i>batchSize</i> can not be fit into the specified <i>maxMessageSize</i>
     * <p>
     * <pre>
     * Inputs:
     *  channel = never empty
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *  each message size = 3 Bytes
     *
     * Expected Output:
     *  number of batches = 2
     *  number of messages in batch 1 = 3
     *  number of messages in batch 2 = 2
     * </pre>
     */
    @Test
    public void testCreateBatchesExceedingSize() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);

        byte[] mockMsgPayload = {'A', 'b', '~'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(2, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries1 = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries1);
        Assert.assertEquals(3, msgEntries1.size());

        List<SendMessageBatchRequestEntry> msgEntries2 = batches.get(1).getEntries();
        Assert.assertNotNull(msgEntries2);
        Assert.assertEquals(2, msgEntries2.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries2);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#createBatches(org.apache.flume.Channel)} method. Tests the case when the
     * specified <i>batchSize</i> can not fit into the specified <i>maxMessageSize</i> and channel gets empty after
     * certain number of events "takes".
     * <p>
     * <pre>
     * Inputs:
     *  channel = 4 Events
     *  batchSize = 5
     *  maxMessageSize = 10 Bytes
     *  each message size = 3 Bytes
     *
     * Expected Output:
     *  number of batches = 2
     *  number of messages in batch 1 = 3
     *  number of messages in batch 2 = 1
     * </pre>
     */
    @Test
    public void testCreateBatchesExceedingSizeLimitedChannel() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);

        byte[] mockMsgPayload = {'^', '@', '~'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent, mockEvent, mockEvent, mockEvent, null);

        List<SendMessageBatchRequest> batches = sqsMsgSender.createBatches(mockChannel);

        Assert.assertNotNull(batches);
        Assert.assertEquals(2, batches.size());

        List<SendMessageBatchRequestEntry> msgEntries1 = batches.get(0).getEntries();
        Assert.assertNotNull(msgEntries1);
        Assert.assertEquals(3, msgEntries1.size());

        List<SendMessageBatchRequestEntry> msgEntries2 = batches.get(1).getEntries();
        Assert.assertNotNull(msgEntries2);
        Assert.assertEquals(1, msgEntries2.size());

        assertCorrectPayloadInEntries(mockMsgPayload, msgEntries2);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#send(org.apache.flume.Channel)} method. Tests the happy path scenario.
     *
     * @throws Exception
     */
    @Test
    public void testSend() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        AmazonSQS mockSqs = Mockito.mock(AmazonSQS.class);
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(new SendMessageBatchResult());
        sqsMsgSender.setAmazonSQS(mockSqs);

        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        sqsMsgSender.send(mockChannel);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#send(org.apache.flume.Channel)} method. Tests the failure scenario when
     * certain messages in the batch failed to be delivered to SQS.
     * <p>
     * <pre>
     * Expected:
     * - No EventDeliveryException is thrown
     * - The BatchSQSMsgSender returns successfully processed events count
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testSendPartialBatchFailure() throws Exception {
        int batchSize = 5;
        int failedMsgCount = 1;
        int expectedSuccessCount = batchSize - failedMsgCount;

        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey",
                batchSize, 100);
        AmazonSQS mockSqs = Mockito.mock(AmazonSQS.class);

        SendMessageBatchResult mockResult = mockBatchResult(batchSize, expectedSuccessCount);

        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResult);
        sqsMsgSender.setAmazonSQS(mockSqs);

        String msgBody = "Some message payload";
        byte[] mockMsgPayload = msgBody.getBytes();
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        int successCount = sqsMsgSender.send(mockChannel);

        Assert.assertEquals(expectedSuccessCount, successCount);
    }

    private SendMessageBatchResult mockBatchResult(int batchSize, int expectedSuccessCount) {
        SendMessageBatchResult mockResult = Mockito.mock(SendMessageBatchResult.class);

        List<SendMessageBatchResultEntry> successfulEntries = new ArrayList<SendMessageBatchResultEntry>();
        for (int i = 0; i < expectedSuccessCount; i++) {
            successfulEntries.add(new SendMessageBatchResultEntry().withId(String.valueOf(i + 1)));
        }
        when(mockResult.getSuccessful()).thenReturn(successfulEntries);
        List<BatchResultErrorEntry> failedEntries = new ArrayList<BatchResultErrorEntry>();
        for (int i = expectedSuccessCount; i < batchSize; i++) {
            failedEntries.add(
                new BatchResultErrorEntry().withId(String.valueOf(i + 1)).withCode("401").withSenderFault(true)
                    .withMessage("Invalid binary character"));
        }
        when(mockResult.getFailed()).thenReturn(failedEntries);
        return mockResult;
    }

    /**
     * Tests the {@link BatchSQSMsgSender#send(org.apache.flume.Channel)} method. Tests the failure scenario when all
     * the messages in the batch failed to be delivered to SQS.
     * <p>
     * Expected: - EventDeliveryException is thrown - EventDeliveryException also contains the failed messages payload
     * in the exception message
     *
     * @throws Exception
     */
    @Test(expected = EventDeliveryException.class)
    public void testSendCompleteBatchFailure() throws Exception {
        int batchSize = 5;
        int failedMsgCount = batchSize;
        int expectedSuccessCount = batchSize - failedMsgCount;

        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey",
                batchSize, 100);
        AmazonSQS mockSqs = Mockito.mock(AmazonSQS.class);

        SendMessageBatchResult mockResult = mockBatchResult(batchSize, expectedSuccessCount);

        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(mockResult);
        sqsMsgSender.setAmazonSQS(mockSqs);

        String msgBody = "Some message payload";
        byte[] mockMsgPayload = msgBody.getBytes();
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        try {
            sqsMsgSender.send(mockChannel);
        }
        catch (EventDeliveryException ede) {
            // Make sure that the original payload is also part of the exception error messsage body
            // to get the failed payloads logged along with errors
            Assert.assertTrue(ede.getMessage().contains(msgBody));
            //rethrow as the test is expecting this exception to be thrown
            throw ede;
        }
    }

    /**
     * Tests the {@link BatchSQSMsgSender#send(org.apache.flume.Channel)} method. Tests the failure scenario when AWS
     * SQS API throws AmazonServiceException.
     *
     * @throws Exception
     */
    @Test(expected = EventDeliveryException.class)
    public void testSendFailureAmazonServiceException() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        AmazonSQS mockSqs = Mockito.mock(AmazonSQS.class);
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(AmazonServiceException.class);
        sqsMsgSender.setAmazonSQS(mockSqs);

        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        sqsMsgSender.send(mockChannel);
    }

    /**
     * Tests the {@link BatchSQSMsgSender#send(org.apache.flume.Channel)} method. Tests the failure scenario when AWS
     * SQS API throws AmazonClientException.
     *
     * @throws Exception
     */
    @Test(expected = EventDeliveryException.class)
    public void testSendFailureAmazonClientException() throws Exception {
        BatchSQSMsgSender sqsMsgSender =
            new BatchSQSMsgSender("https://some-fake/url", "us-east-1", "someAwsAccessKey", "someAwsSecretKey", 5, 10);
        AmazonSQS mockSqs = Mockito.mock(AmazonSQS.class);
        when(mockSqs.sendMessageBatch(any(SendMessageBatchRequest.class))).thenThrow(AmazonClientException.class);
        sqsMsgSender.setAmazonSQS(mockSqs);

        byte[] mockMsgPayload = {'A', 'b'};
        Event mockEvent = Mockito.mock(Event.class);
        when(mockEvent.getBody()).thenReturn(mockMsgPayload);

        Channel mockChannel = Mockito.mock(Channel.class);
        when(mockChannel.take()).thenReturn(mockEvent);

        sqsMsgSender.send(mockChannel);
    }

    private void assertCorrectPayloadInEntries(byte[] mockMsgPayload, List<SendMessageBatchRequestEntry> msgEntries)
        throws UnsupportedEncodingException {
        for (SendMessageBatchRequestEntry entry : msgEntries) {
            Assert.assertNotNull(entry);
            Assert.assertEquals(new String(mockMsgPayload, "UTF-8"), entry.getMessageBody());
        }
    }
}
