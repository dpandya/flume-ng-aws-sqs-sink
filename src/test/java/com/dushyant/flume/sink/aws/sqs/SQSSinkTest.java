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

import static org.junit.Assert.assertEquals;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.instrumentation.SinkCounter;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author dpandya
 */
public class SQSSinkTest {
    /**
     * Tests the {@link SQSSink#process()} method. Tests happy path scenario when one batch size worth of messages are
     * successfully sent to SQS.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  txnEventCount (number of events processed) = 5
     *
     * Expected Result:
     *  status = READY
     *  transaction.begin() is invoked
     *  transaction.commit() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is invoked
     *  sinkCounter.incrementBatchUnderflowCount() is NOT invoked
     *  sinkCounter.incrementBatchEmptyCount() is NOT invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is invoked with txnEventCount=5
     *  counterGroup.incrementAndGet("transaction.success") is invoked
     *
     * </pre>
     */
    @Test
    public void testProcessCompletedBatch() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        int mockSuccessfullySentEventCount = 5;
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenReturn(mockSuccessfullySentEventCount);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        Sink.Status status = sqsSink.process();
        assertEquals(Sink.Status.READY, status);
        Mockito.verify(mockTransaction, Mockito.times(1)).begin();
        Mockito.verify(mockTransaction, Mockito.times(1)).commit();
        Mockito.verify(mockTransaction, Mockito.times(1)).close();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).incrementBatchCompleteCount();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchUnderflowCount();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchEmptyCount();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).addToEventDrainSuccessCount(mockSuccessfullySentEventCount);
        Mockito.verify(mockCounterGroup, Mockito.times(1)).incrementAndGet("transaction.success");
    }

    /**
     * Tests the {@link SQSSink#process()} method. Tests the scenario when there are less messages in channel than one
     * batch size.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  txnEventCount (number of events processed) = 3
     *
     * Expected Result:
     *  status = READY
     *  transaction.begin() is invoked
     *  transaction.commit() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is NOT invoked
     *  sinkCounter.incrementBatchUnderflowCount() is invoked
     *  sinkCounter.incrementBatchEmptyCount() is NOT invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is invoked with txnEventCount=3
     *  counterGroup.incrementAndGet("transaction.success") is invoked
     *
     * </pre>
     */
    @Test
    public void testProcessBatchUnderflow() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        int mockSuccessfullySentEventCount = 3;
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenReturn(mockSuccessfullySentEventCount);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        Sink.Status status = sqsSink.process();
        assertEquals(Sink.Status.READY, status);
        Mockito.verify(mockTransaction, Mockito.times(1)).begin();
        Mockito.verify(mockTransaction, Mockito.times(1)).commit();
        Mockito.verify(mockTransaction, Mockito.times(1)).close();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchCompleteCount();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).incrementBatchUnderflowCount();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchEmptyCount();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).addToEventDrainSuccessCount(mockSuccessfullySentEventCount);
        Mockito.verify(mockCounterGroup, Mockito.times(1)).incrementAndGet("transaction.success");
    }

    /**
     * Tests the {@link SQSSink#process()} method. Tests the scenario when there are no more messages in the channel.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  txnEventCount (number of events processed) = 0
     *
     * Expected Result:
     *  status = BACKOFF
     *  transaction.begin() is invoked
     *  transaction.commit() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is NOT invoked
     *  sinkCounter.incrementBatchUnderflowCount() is NOT invoked
     *  sinkCounter.incrementBatchEmptyCount() is invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is invoked with txnEventCount=3
     *  counterGroup.incrementAndGet("transaction.success") is invoked
     *
     * </pre>
     */
    @Test
    public void testProcessBatchEmpty() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        int mockSuccessfullySentEventCount = 0;
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenReturn(mockSuccessfullySentEventCount);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        Sink.Status status = sqsSink.process();
        assertEquals(Sink.Status.BACKOFF, status);
        Mockito.verify(mockTransaction, Mockito.times(1)).begin();
        Mockito.verify(mockTransaction, Mockito.times(1)).commit();
        Mockito.verify(mockTransaction, Mockito.times(1)).close();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchCompleteCount();
        Mockito.verify(mockSinkCounter, Mockito.times(0)).incrementBatchUnderflowCount();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).incrementBatchEmptyCount();
        Mockito.verify(mockSinkCounter, Mockito.times(1)).addToEventDrainSuccessCount(mockSuccessfullySentEventCount);
        Mockito.verify(mockCounterGroup, Mockito.times(1)).incrementAndGet("transaction.success");
    }

    /**
     * Tests the {@link SQSSink#process()} method. Tests the scenario when there is EventDeliveryException when sending
     * events to SQS.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  EventDeliveryException is thrown from message sender
     *
     * Expected Result:
     *  transaction.begin() is invoked
     *  transaction.rollback() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is NOT invoked
     *  sinkCounter.incrementBatchUnderflowCount() is NOT invoked
     *  sinkCounter.incrementBatchEmptyCount() is NOT invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is NOT invoked
     *  counterGroup.incrementAndGet("transaction.success") is NOT invoked
     *  EventDeliveryException is rethrown from the sink
     *
     * </pre>
     */
    @Test(expected = EventDeliveryException.class)
    public void testProcessEventDeliveryException() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenThrow(EventDeliveryException.class);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        sqsSink.process();
    }


    /**
     * Tests the {@link SQSSink#process()} method. Tests the scenario when there is an Exception when sending events to
     * SQS.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  An Exception is thrown from message sender
     *
     * Expected Result:
     *  transaction.begin() is invoked
     *  transaction.rollback() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is NOT invoked
     *  sinkCounter.incrementBatchUnderflowCount() is NOT invoked
     *  sinkCounter.incrementBatchEmptyCount() is NOT invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is NOT invoked
     *  counterGroup.incrementAndGet("transaction.success") is NOT invoked
     *  EventDeliveryException is rethrown from the sink
     *
     * </pre>
     */
    @Test(expected = EventDeliveryException.class)
    public void testProcessException() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenThrow(Exception.class);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        sqsSink.process();
    }

    /**
     * Tests the {@link SQSSink#process()} method. Tests the scenario when there is an Error when sending events to
     * SQS.
     * <p>
     * <pre>
     * Inputs:
     *  batchSize = 5
     *  An Error is thrown from message sender
     *
     * Expected Result:
     *  transaction.begin() is invoked
     *  transaction.rollback() is invoked
     *  transaction.close() is invoked
     *  sinkCounter.incrementBatchCompleteCount() is NOT invoked
     *  sinkCounter.incrementBatchUnderflowCount() is NOT invoked
     *  sinkCounter.incrementBatchEmptyCount() is NOT invoked
     *  sinkCounter.addToEventDrainSuccessCount(txnEventCount) is NOT invoked
     *  counterGroup.incrementAndGet("transaction.success") is NOT invoked
     *  The same Error is rethrown from the sink
     *
     * </pre>
     */
    @Test(expected = Error.class)
    public void testProcessError() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenThrow(Error.class);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getString("awsAccessKey", "env.AWS_ACCESS_KEY")).thenReturn("SOME FAKE ACCESS KEY");
        Mockito.when(mockContext.getString("awsSecretKey", "env.AWS_SECRET_KEY")).thenReturn("SOME FAKE SECRET KEY");
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);

        Channel mockChannel = Mockito.mock(Channel.class);
        Transaction mockTransaction = Mockito.mock(Transaction.class);
        Mockito.when(mockChannel.getTransaction()).thenReturn(mockTransaction);
        Mockito.when(sqsSink.getChannel()).thenReturn(mockChannel);

        SinkCounter mockSinkCounter = Mockito.mock(SinkCounter.class);
        sqsSink.setSinkCounter(mockSinkCounter);
        CounterGroup mockCounterGroup = Mockito.mock(CounterGroup.class);
        sqsSink.setCounterGroup(mockCounterGroup);

        sqsSink.process();
    }


    /**
     * Tests the {@link SQSSink#configure(Context)} } method. Tests the case when no accesskey or secretkey are
     * specified. The sink should rely on AWS SDK configuration [http://docs.aws.amazon
     * .com/AWSSdkDocsJava/latest//DeveloperGuide/credentials.html] or instance role in that case.
     * <p>
     * <pre>
     * Inputs:
     * No awsAccessKey or awsSecretKey specified
     *
     * Expected Result:
     * sqsSink configuration should succeed and it should not throw any exception
     * </pre>
     */
    @Test
    public void testConfigureNoAccessKeySecretKey() throws Exception {
        SQSSink underlyingSqsSink = new SQSSink();
        SQSSink sqsSink = Mockito.spy(underlyingSqsSink);

        SQSMsgSender mockSqsMsgSender = Mockito.mock(SQSMsgSender.class);
        Mockito.when(mockSqsMsgSender.send(Mockito.any(Channel.class))).thenThrow(Error.class);
        sqsSink.setSqsMsgSender(mockSqsMsgSender);

        Context mockContext = Mockito.mock(Context.class);
        Mockito.when(mockContext.getString("sqsUrl", null)).thenReturn("http://some.fake.url");
        Mockito.when(mockContext.getInteger("batchSize", 10)).thenReturn(5);
        Mockito.when(mockContext.getString("region", "us-east-1")).thenReturn(null);
        Mockito.when(mockContext.getLong("maxMessageSize", 256L * 1024L)).thenReturn(256L * 1024L);
        sqsSink.configure(mockContext);
    }
}
