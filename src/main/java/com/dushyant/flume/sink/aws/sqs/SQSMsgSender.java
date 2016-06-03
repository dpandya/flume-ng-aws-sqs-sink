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

import org.apache.flume.Channel;
import org.apache.flume.EventDeliveryException;

/**
 * An internal message sender interface for sending messages to <a href="http://aws.amazon.com/sqs/">Amazon Simple
 * Queue Service (Amazon SQS).</a>. The sender takes event(s) from the specified channel and sends them to SQS.
 *
 * @author dpandya
 */
public interface SQSMsgSender {
    /**
     * Takes events from the specified channel and sends them to Amazon SQS until the Channel is empty or until
     * configured batchSize is reached (whichever occurs first). The method returns the number of events successfully
     * taken from the channel and sent to the SQS by this method call.
     * <p>
     * The concrete implementations are NOT expected to manage transaction. It is assumed that the transaction
     * management is taken care of by the calling Sink class.
     *
     * @param channel The flume channel to pick events from
     *
     * @return int - Number of events sent to SQS by this method call.
     * @throws EventDeliveryException If a message delivery to SQS fails.
     */
    public int send(Channel channel) throws EventDeliveryException;
}
