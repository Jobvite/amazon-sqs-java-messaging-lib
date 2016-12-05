package com.amazon.sqs.javamessaging;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;

import com.amazon.sqs.javamessaging.acknowledge.SQSMessageRetryMode;
import com.amazon.sqs.javamessaging.acknowledge.SQSMessageRetryMode.RetryMode;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public class ExtendedSQSConnectionFactory implements ConnectionFactory, QueueConnectionFactory {
    private final ClientConfiguration clientConfig;
    private final Region region;
    private final String endpoint;
    private final String signerRegionOverride;
    private final AWSCredentialsProvider awsCredentialsProvider;
    private AmazonSQS sqsClient;
    private final SQSMessageRetryMode sqsMessageRetryMode;

    /** Controls the size of the prefetch buffers used by consumers. */
    private final int numberOfMessagesToPrefetch;

    private ExtendedSQSConnectionFactory(Builder builder) {
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.signerRegionOverride = builder.signerRegionOverride;
        this.clientConfig = builder.clientConfiguration;
        this.awsCredentialsProvider = builder.awsCredentialsProvider;
        this.numberOfMessagesToPrefetch = builder.numberOfMessagesToPrefetch;
        this.sqsClient = builder.sqsClient;
        this.sqsMessageRetryMode = new SQSMessageRetryMode(builder.retryMode, builder.retryDelay);
    }

    @Override
    public SQSConnection createConnection() throws JMSException {
        if (awsCredentialsProvider == null) {
            throw new JMSSecurityException("AWS credentials cannot be null");
        }
        return createConnection(awsCredentialsProvider);
    }

    @Override
    public SQSConnection createConnection(String awsAccessKeyId, String awsSecretKey) throws JMSException {
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretKey);
        return createConnection(basicAWSCredentials);
    }

    public SQSConnection createConnection(AWSCredentials awsCredentials) throws JMSException {
        if(sqsClient == null){
            sqsClient = new AmazonSQSClient(awsCredentials, clientConfig);
        }
        configureClient(sqsClient);
        AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper = new AmazonSQSMessagingClientWrapper(sqsClient);
        return new SQSConnection(amazonSQSClientJMSWrapper, numberOfMessagesToPrefetch, sqsMessageRetryMode);
    }
    
    public SQSConnection createConnection(AWSCredentialsProvider awsCredentialsProvider) throws JMSException {
        if(sqsClient == null){
            sqsClient = new AmazonSQSClient(awsCredentialsProvider, clientConfig);
        }
        configureClient(sqsClient);
        AmazonSQSMessagingClientWrapper amazonSQSClientJMSWrapper = new AmazonSQSMessagingClientWrapper(sqsClient);
        return new SQSConnection(amazonSQSClientJMSWrapper, numberOfMessagesToPrefetch, sqsMessageRetryMode);
    }
    
    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return (QueueConnection) createConnection();
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return (QueueConnection) createConnection(userName, password);
    }
    
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Region region;
        private String endpoint;
        private String signerRegionOverride;
        private ClientConfiguration clientConfiguration;
        private int numberOfMessagesToPrefetch;
        private AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        private AmazonSQS sqsClient;
        private RetryMode retryMode = RetryMode.RETRY_MODE_DEFAULT_DELAY;
        private int retryDelay = 0;
        
        public Builder(Region region) {
            this();
            this.region = region;
        }

        /** Recommended way to set the AmazonSQSClient is to use region */
        public Builder(String region) {
            this(Region.getRegion(Regions.fromName(region)));
        }

        public Builder() {
            clientConfiguration = new ClientConfiguration();
            clientConfiguration.setUserAgent(
                    clientConfiguration.getUserAgent() + SQSMessagingClientConstants.APPENDED_USER_AGENT_HEADER_VERSION );
            
            // Set default numberOfMessagesToPrefetch to MIN_BATCH.
            this.numberOfMessagesToPrefetch = SQSMessagingClientConstants.MIN_BATCH;
        }
        
        public Builder withRegion(Region region) {
            setRegion(region);
            return this;
        }
        
        public Builder withRegionName(String regionName) 
            throws IllegalArgumentException
        {
            setRegion(Region.getRegion( Regions.fromName(regionName) ) );
            return this;
        }
        
        public Builder withEndpoint(String endpoint) {
            setEndpoint(endpoint);
            return this;
        }
        
        /**
         * An internal method used to explicitly override the internal signer region
         * computed by the default implementation. This method is not expected to be
         * normally called except for AWS internal development purposes.
         */
        public Builder withSignerRegionOverride(String signerRegionOverride) {
            setSignerRegionOverride(signerRegionOverride);
            return this;
        }
        
        public Builder withAWSCredentialsProvider(AWSCredentialsProvider awsCredentialsProvider) {
            setAwsCredentialsProvider(awsCredentialsProvider);
            return this;
        }

        public Builder withClientConfiguration(ClientConfiguration clientConfig) {
            setClientConfiguration(clientConfig);
            return this;
        }

        public Builder withNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
            setNumberOfMessagesToPrefetch(numberOfMessagesToPrefetch);
            return this;
        }
        
        public Builder withSqsClient(AmazonSQS sqsClient) {
            setSqsClient(sqsClient);
            return this;
        }

        public Builder withRetryMode(RetryMode retryMode) {
            setRetryMode(retryMode);
            return this;
        }

        public Builder withRetryDelay(int retryDelay) {
            setRetryDelay(retryDelay);
            return this;
        }

        public ExtendedSQSConnectionFactory build() {
            return new ExtendedSQSConnectionFactory(this);
        }

        public Region getRegion() {
            return region;
        }

        public void setRegion(Region region) {
            this.region = region;
            this.endpoint = null;
        }
        
        public void setRegionName(String regionName) {
            setRegion( Region.getRegion( Regions.fromName( regionName ) ) );
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            this.region = null;
        }

        public String getSignerRegionOverride() {
            return signerRegionOverride;
        }

        public void setSignerRegionOverride(String signerRegionOverride) {
            this.signerRegionOverride = signerRegionOverride;
        }

        public ClientConfiguration getClientConfiguration() {
            return clientConfiguration;
        }

        public void setClientConfiguration(ClientConfiguration clientConfig) {
            clientConfiguration = new ClientConfiguration( clientConfig );
            if( clientConfig.getUserAgent() == null || clientConfig.getUserAgent().isEmpty() ) {
                clientConfig.setUserAgent( ClientConfiguration.DEFAULT_USER_AGENT );
            }
            clientConfiguration.setUserAgent(
                    clientConfig.getUserAgent() + SQSMessagingClientConstants.APPENDED_USER_AGENT_HEADER_VERSION );
        }

        public int getNumberOfMessagesToPrefetch() {
            return numberOfMessagesToPrefetch;
        }

        public void setNumberOfMessagesToPrefetch(int numberOfMessagesToPrefetch) {
            if (numberOfMessagesToPrefetch <= 0 ) {
                throw new IllegalArgumentException("Invalid prefetch size.");
            }
            this.numberOfMessagesToPrefetch = numberOfMessagesToPrefetch;
        }

        public AWSCredentialsProvider getAwsCredentialsProvider() {
            return awsCredentialsProvider;
        }

        public void setAwsCredentialsProvider(
                AWSCredentialsProvider awsCredentialsProvider) {
            this.awsCredentialsProvider = awsCredentialsProvider;
        }

        public AmazonSQS getSqsClient() {
            return sqsClient;
        }

        public void setSqsClient(AmazonSQS sqsClient) {
            this.sqsClient = sqsClient;
        }
        public RetryMode getRetryMode()
        {
            return retryMode;
        }

        public void setRetryMode(RetryMode retryMode)
        {
            this.retryMode = retryMode;
        }

        public int getRetryDelay()
        {
            return retryDelay;
        }

        public void setRetryDelay(int retryDelay)
        {
            this.retryDelay = retryDelay;
        }
    }
    
    private void configureClient(AmazonSQS client) throws JMSException {
        try {
            if( region != null ) {
                client.setRegion(region);
            }
            if( endpoint != null ) {
                client.setEndpoint(endpoint);
            }
            if( signerRegionOverride != null && client instanceof AmazonSQSClient ) {
                ((AmazonSQSClient)client).setSignerRegionOverride(signerRegionOverride);
            }
        } catch( IllegalArgumentException e ) {
            throw (JMSException)
                new JMSException( "Bad endpoint configuration: " + e.getMessage() ).initCause(e);
        }
    }
}
