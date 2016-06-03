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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <a href="https://flume.apache.org/">Apache Flume</a> sink for <a href="http://aws.amazon.com/sqs/">Amazon Simple
 * Queue Service (Amazon SQS).</a>. <b>Configuration Properties:</b> <table summary="" border=1>
 * <tr><td><b>Name</b></td><td><b>Default</b></td><td><b>Description</b></td></tr>
 * <tr><td>sqsUrl</td><td>&nbsp;</td><td>The url of the SQS to send the messages to</td></tr>
 * <tr><td>region</td><td>us-east-1</td><td>The AWS region</td></tr> <tr><td>awsAccessKey</td><td>env
 * .AWS_ACCESS_KEY</td><td>The AWS api access key id. This is optional. If {@code awsAccessKey} or {@code awsSecretKey}
 * are not specified then the sink will use the {@link DefaultAWSCredentialsProviderChain} to look for AWS credentials.
 * The {@code awsAccessKey} can also be specified in {@code env.variableName} format; in that case, the value of system
 * environment variable {@code variableName} will be used.</td></tr> <tr><td>awsSecretKey</td><td>env
 * .AWS_SECRET_KEY</td><td>The AWS api access secret key. This is optional. If {@code awsAccessKey} or {@code
 * awsSecretKey} are not specified then the sink will use the {@link DefaultAWSCredentialsProviderChain} to look for AWS
 * credentials. The {@code awsSecretKey} can also be specified in {@code env.variableName} format; in that case, the
 * value of system environment variable {@code variableName} will be used.</td></tr>
 * <tr><td>batchSize</td><td>10</td><td>Number of messages to be sent in one batch. This should be between 1 and 10
 * (inclusive). AWS SQS allows max of 10 messages per batch.</td></tr> <tr><td>maxMessageSize</td><td>262144</td><td>The
 * max size of a message or batch (in bytes). Currently AWS allows max of 256KB.</td></tr>
 * <tr><td>alwaysBatch</td><td>true</td><td>AWS SQS allows two separate APIs for sending a message to SQS. <a
 * href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html">SendMessage</a> and
 * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html">
 * SendMessageBatch</a>. This flag indicates usage of SendMessageBatch API when <i>batchSize=1</i>. This flag is
 * applicable only when {@code batchSize=1}. It will be ignored for all cases of {@code batchSize>1}. For example
 * when,<ul><li>{@code batchSize=1,alwaysBatch=true}: SendMessageBatch will be used</li><li><i>batchSize=1,
 * alwaysBatch=false</i>: SendMessage will be used</li><li>{@code batchSize>1,alwaysBatch=true|false}: SendMessageBatch
 * will be used</li></ul></td></tr> </table>
 * <p>
 * Here is a sample flume config for the sink that uses system environment variables named {@code AWS_ACCESS_KEY} and
 * {@code AWS_SECRET_KEY} to set aws credentials.
 * <pre>
 * {@code
 *
 * agent.sources = s1
 * agent.channels = c1
 * agent.sinks = k1
 *
 * agent.sinks.k1.type = com.dushyant.flume.sink.aws.sqs.SQSSink
 * agent.sinks.k1.channel = c1
 * agent.sinks.k1.sqsUrl = https://sqs.us-east-1.amazonaws.com/12345646/some-sqs-name
 * agent.sinks.k1.awsAccessKey = env.AWS_ACCESS_KEY
 * agent.sinks.k1.awsSecretKey = env.AWS_SECRET_KEY
 * agent.sinks.k1.region = us-east-1
 * agent.sinks.k1.batchSize = 10
 * agent.sinks.k1.alwaysSendBatches = true
 * }
 * </pre>
 * <p>
 * Here is another sample flume config for the sink that relies on {@link DefaultAWSCredentialsProviderChain} to look
 * for aws credentials in the following order <ul><li>Environment Variables - <code>AWS_ACCESS_KEY_ID</code> and
 * <code>AWS_SECRET_ACCESS_KEY</code> (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for
 * .NET), or <code>AWS_ACCESS_KEY</code> and <code>AWS_SECRET_KEY</code> (only recognized by Java SDK) </li> <li>Java
 * System Properties - aws.accessKeyId and aws.secretKey</li> <li>Credential profiles file at the default location
 * (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI</li> <li>Instance profile credentials delivered through
 * the Amazon EC2 metadata service</li> </ul>
 * <pre>
 * {@code
 *
 * agent.sources = s1
 * agent.channels = c1
 * agent.sinks = k1
 *
 * agent.sinks.k1.type = com.dushyant.flume.sink.aws.sqs.SQSSink
 * agent.sinks.k1.channel = c1
 * agent.sinks.k1.sqsUrl = https://sqs.us-east-1.amazonaws.com/12345646/some-sqs-name
 * agent.sinks.k1.region = us-east-1
 * agent.sinks.k1.batchSize = 10
 * agent.sinks.k1.alwaysSendBatches = true
 * }
 * </pre>
 *
 * @author dpandya
 */
public class SQSSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(SQSSink.class);

    // The default max message size in Kb. Amazon currently allows payload size per message or per batch as 256KB.
    private static final int DEFAULT_MAX_MSG_SIZE_KB = 256;
    private static final long DEFAULT_MAX_MSG_SIZE_BYTES = 256L * 1024L;

    private SQSMsgSender sqsMsgSender;

    // Internal counters for sink related metrics collection. Used for Flume Monitoring.
    private SinkCounter sinkCounter;

    private CounterGroup counterGroup = new CounterGroup();
    private int batchSize;

    @Override
    public void start() {
        sinkCounter.start();
        super.start();
    }

    @Override
    public void stop() {
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public void configure(Context context) {
        final String sqsUrl = resolve(context.getString("sqsUrl", null));
        Preconditions.checkState(sqsUrl != null, "SQS url not configured.");
        LOG.debug("sqsUrl:{}", sqsUrl);

        final String region = resolve(context.getString("region", "us-east-1"));
        LOG.debug("region:{}", region);

        final String awsAccessKey = resolve(context.getString("awsAccessKey", "env.AWS_ACCESS_KEY"));
        final String awsSecretKey = resolve(context.getString("awsSecretKey", "env.AWS_SECRET_KEY"));
        LOG.debug("awsAccessKey:{}", awsAccessKey);

        this.batchSize = context.getInteger("batchSize", 10);
        Preconditions.checkState(1 <= batchSize && batchSize <= 10,
            "Invalid batchSize specified. The batchSize must be a positive integer between 1 and 10 " +
                "(inclusive). The batch size 1 means no batching and each event will be attempted to be sent " +
                "to SQS as soon as received by the sink. AWS SQS allows max of 10 messages per batch.");
        LOG.debug("batchSize:{}", batchSize);

        final long maxMessageSize = context.getLong("maxMessageSize", DEFAULT_MAX_MSG_SIZE_BYTES);
        Preconditions.checkState(1 <= maxMessageSize && maxMessageSize <= DEFAULT_MAX_MSG_SIZE_BYTES,
            "Invalid maxMessageSize specified. The maxMessageSize must be " +
                "between 1 and " + DEFAULT_MAX_MSG_SIZE_BYTES +
                "(inclusive). Currently, Amazon allows max payload size of " + DEFAULT_MAX_MSG_SIZE_KB +
                "KB per request or per batch.");
        LOG.debug("maxMessageSize:{}", maxMessageSize);

        final boolean alwaysBatch = context.getBoolean("alwaysBatch", true);
        LOG.debug("alwaysBatch:{}", alwaysBatch);

        // TODO: Add dynamic reconfiguration support
        if (sqsMsgSender == null) {
            if (alwaysBatch || batchSize > 1) {
                LOG.info("Using sqsMsgSender:{}", BatchSQSMsgSender.class);
                sqsMsgSender =
                    new BatchSQSMsgSender(sqsUrl, region, awsAccessKey, awsSecretKey, batchSize, maxMessageSize);
            }
            else {
                LOG.info("Using sqsMsgSender:{}", BasicSQSMsgSender.class);
                sqsMsgSender = new BasicSQSMsgSender(sqsUrl, region, awsAccessKey, awsSecretKey);
            }
        }

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    /**
     * The method for resolving the configured property value.
     * <p>
     * For example, the configuration allows properties to be specified in the {@code env.propertyName} format. This
     * method resolves that configuration property value using the environment variable named {@code propertyName}. In
     * other cases, the method returns the given property as is.
     *
     * @param property The configured property value to be resolved
     *
     * @return The resolved property value
     */
    protected String resolve(String property) {
        String resolved = property;
        if (StringUtils.isNotBlank(property) && property.startsWith("env.")) {
            String envVariableName = StringUtils.substringAfter(property, "env.");
            resolved = System.getenv(envVariableName);
        }
        return resolved;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            // Call the sender to take and send events to SQS. The method call below returns the number of events
            // sent to SQS in one call.
            int txnEventCount = sqsMsgSender.send(channel);
            status = Status.READY;

            // Increment sinkCounter appropriately
            if (txnEventCount == 0) {
                // Return status of BACKOFF when no events were processed. This will cause flume to "backoff" from
                // the sink for sometime before trying again.
                // Usually happens when the channel is empty.
                status = Status.BACKOFF;
                sinkCounter.incrementBatchEmptyCount();
            }
            else if (txnEventCount == batchSize) {
                // Entire batch is processed in this transaction. Increment the complete batch count.
                sinkCounter.incrementBatchCompleteCount();
            }
            else {
                // There were less events to process in this transaction then the batch size. Increment the batch
                // underflow count.
                sinkCounter.incrementBatchUnderflowCount();
            }

            // No exceptions or errors so happily commit the transaction
            transaction.commit();

            // The "txnEventCount" number of events are successfully drained (i.e.,
            // sent to SQS already) so increment the drain success count
            sinkCounter.addToEventDrainSuccessCount(txnEventCount);
            counterGroup.incrementAndGet("transaction.success");
        }
        catch (EventDeliveryException ede) {
            // Rollback and rethrow event delivery exception
            rollback(transaction);
            throw ede;
        }
        catch (Exception e) {
            // Rollback and convert to event delivery exception and throw
            rollback(transaction);
            throw new EventDeliveryException("Exception while sending message(s) to SQS", e);
        }
        catch (Error err) {
            // Rollback and rethrow error
            rollback(transaction);
            throw err;
        }
        finally {
            transaction.close();
        }
        return status;
    }

    private void rollback(Transaction transaction) {
        transaction.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
    }

    public void setSqsMsgSender(SQSMsgSender sqsMsgSender) {
        this.sqsMsgSender = sqsMsgSender;
    }

    protected void setSinkCounter(SinkCounter sinkCounter) {
        this.sinkCounter = sinkCounter;
    }

    protected void setCounterGroup(CounterGroup counterGroup) {
        this.counterGroup = counterGroup;
    }
}
