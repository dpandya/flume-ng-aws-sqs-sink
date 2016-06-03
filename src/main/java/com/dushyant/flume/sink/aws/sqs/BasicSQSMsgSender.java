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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic writer implementation for {@link SQSMsgSender}. This writer does not support batch messages and writes one
 * message at a time to the SQS using
 * <p>
 * <a href="http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html">SendMessage</a>
 * AWS SQS API.
 *
 * @author dpandya
 */
public class BasicSQSMsgSender implements SQSMsgSender {
    private static final Logger LOG = LoggerFactory.getLogger(BasicSQSMsgSender.class);

    private String sqsUrl = null;
    private String region = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;

    private AmazonSQS amazonSQS = null;

    public BasicSQSMsgSender(String sqsUrl, String region, String awsAccessKey, String awsSecretKey) {
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

        this.amazonSQS = sqs;
    }

    @Override
    public int send(Channel channel) throws EventDeliveryException {
        int eventProcessedCounter = 0;
        Event event = channel.take();
        if (event == null || event.getBody().length == 0) {
            // Don't bother with anything if the channel returns null event or an event with empty body
            return eventProcessedCounter;
        }
        try {
            this.amazonSQS.sendMessage(new SendMessageRequest(sqsUrl, new String(event.getBody(), "UTF-8").trim()));
            // This event is processed successfully to increment the counter
            eventProcessedCounter++;
        }
        catch (AmazonServiceException ase) {
            throw new EventDeliveryException("Failure sending message request to Amazon SQS, " +
                "the request made it to SQS but was rejected for some reason.", ase);
        }
        catch (AmazonClientException ace) {
            throw new EventDeliveryException("Failure sending message request to Amazon SQS.", ace);
        }
        catch (UnsupportedEncodingException e) {
            throw new EventDeliveryException("Character set UTF-8 not supported.", e);
        }
        return eventProcessedCounter;
    }

    public void setAmazonSQS(AmazonSQS amazonSQS) {
        this.amazonSQS = amazonSQS;
    }
}
