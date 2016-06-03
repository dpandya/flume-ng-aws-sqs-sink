[Apache Flume](https://flume.apache.org/) sink for [Amazon Simple Queue Service (Amazon SQS).](http://aws.amazon.com/sqs/). 

**Configuration Properties:**

* `sqsUrl`: The url of the SQS to send the messages to
* `region`: The AWS region. Default: `us-east-1`. 
* `awsAccessKey`: The AWS api access key id. This is optional. If `awsAccessKey` or `awsSecretKey` are not specified then the sink will use the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to look for AWS credentials. The `awsAccessKey` can also be specified in `env.variableName` format; in that case, the value of system environment variable `variableName` will be used.
* `awsSecretKey`: The AWS api access secret key. This is optional. If `awsAccessKey` or `awsSecretKey` are not specified then the sink will use the [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to look for AWS credentials. The `awsSecretKey` can also be specified in `env.variableName` format; in that case, the value of system environment variable `variableName` will be used.
* `batchSize`: Number of messages to be sent in one batch. This should be between 1 and 10 (inclusive). AWS SQS allows max of 10 messages per batch. Default: `10`
* `maxMessageSize`: The max size of a message or batch (in bytes). Currently AWS allows max of `256KB`. Default: `262144`
* `alwaysBatch`: AWS SQS allows two separate APIs for sending a message to SQS. [SendMessage](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html) and [SendMessageBatch](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html). This flag indicates usage of SendMessageBatch API when `batchSize=1`. This flag is applicable only when `batchSize=1`. It will be ignored for all cases of `batchSize>1`. Default: `true`. For example when,
..*   `batchSize=1, alwaysBatch=true`: SendMessageBatch will be used
..*   `batchSize=1, alwaysBatch=false`: SendMessage will be used
..*   `batchSize>1, alwaysBatch=true|false`: SendMessageBatch will be used

Here is a sample flume config for the sink that uses system environment variables named `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` to set aws credentials.

<pre>
agent.sources = s1
agent.channels = c1
agent.sinks = k1

agent.sinks.k1.type = com.dushyant.flume.sink.aws.sqs.SQSSink
agent.sinks.k1.channel = c1
agent.sinks.k1.sqsUrl = https://sqs.us-east-1.amazonaws.com/12345646/some-sqs-name
agent.sinks.k1.awsAccessKey = env.AWS_ACCESS_KEY
agent.sinks.k1.awsSecretKey = env.AWS_SECRET_KEY
agent.sinks.k1.region = us-east-1
agent.sinks.k1.batchSize = 10
agent.sinks.k1.alwaysSendBatches = true 
</pre>

Here is another sample flume config for the sink that relies on [DefaultAWSCredentialsProviderChain](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to look for aws credentials in the following order

*   Environment Variables - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or `AWS_ACCESS_KEY` and `AWS_SECRET_KEY` (only recognized by Java SDK)
*   Java System Properties - `aws.accessKeyId` and `aws.secretKey`
*   Credential profiles file at the default location (`~/.aws/credentials`) shared by all AWS SDKs and the AWS CLI
*   Instance profile credentials delivered through the Amazon EC2 metadata service

<pre>
agent.sources = s1
agent.channels = c1
agent.sinks = k1

agent.sinks.k1.type = com.dushyant.flume.sink.aws.sqs.SQSSink
agent.sinks.k1.channel = c1
agent.sinks.k1.sqsUrl = https://sqs.us-east-1.amazonaws.com/12345646/some-sqs-name
agent.sinks.k1.region = us-east-1
agent.sinks.k1.batchSize = 10
agent.sinks.k1.alwaysSendBatches = true
</pre>