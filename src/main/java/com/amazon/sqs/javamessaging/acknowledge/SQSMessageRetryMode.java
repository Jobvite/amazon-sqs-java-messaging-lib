/*
 * Copyright 2010-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging.acknowledge;

public class SQSMessageRetryMode
{

    /**
     * <P>
     * Specifies the different possible sqs message retry modes:
     * <ul>
     * <li>In RETRY_MODE_DEFAULT_DELAY mode, it uses the current aws jms message retry mode which is to
     * reset AWS SQS message VisibilityTimeout to 0 to be retried immediately.</li>
     * <li>In RETRY_MODE_EXPLICIT_DELAY mode, use an explicit delay value to set the AWS SQS
     * VisibilityTimeout for in seconds.</li>
     * <li>In RETRY_MODE_QUEUE_DELAY mode, use the existing VisibilityTimeout already set for AWS SQS
     * queue.</li>
     * </ul>
     */
    public enum RetryMode {
        RETRY_MODE_DEFAULT_DELAY,
        RETRY_MODE_EXPLICIT_DELAY,
        RETRY_MODE_QUEUE_DELAY;
    }

    private final RetryMode retryMode;
    private final int retryDelay;

    public RetryMode getRetryMode()
    {
        return retryMode;
    }

    public int getRetryDelay()
    {
        return retryDelay;
    }
    
    public SQSMessageRetryMode(RetryMode retryMode, int retryDelay)
    {
        this.retryMode = retryMode;
        this.retryDelay = retryDelay;
    }
}