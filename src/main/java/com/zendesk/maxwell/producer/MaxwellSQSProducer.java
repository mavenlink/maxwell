package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellSQSPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.net.URI;

public class MaxwellSQSProducer extends AbstractAsyncProducer {

	private SqsAsyncClient client;
	private String queueUri;
	private MaxwellSQSPartitioner partitioner;

	public MaxwellSQSProducer(MaxwellContext context, String queueUri, String serviceEndpoint, String signingRegion) {
		super(context);
		this.queueUri = queueUri;
		this.client = SqsAsyncClient.builder()
				.endpointOverride(URI.create(serviceEndpoint))
				.region(Region.of(signingRegion))
				.build();
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellSQSPartitioner(partitionKey, partitionColumns, partitionFallback);
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String value = r.toJSON(outputConfig);

		SendMessageRequest.Builder builder = SendMessageRequest.builder()
				.queueUrl(queueUri)
				.messageBody(value);

		if (queueUri.endsWith(".fifo")) {
			String key = this.partitioner.getSQSKey(r);
			builder.messageGroupId(key);
		}

		SendMessageRequest messageRequest = builder.build();
		SQSCallback callback = new SQSCallback(cc, r.getNextPosition(), value, context);
		client.sendMessage(messageRequest).whenComplete(callback);
	}

}

class SQSCallback implements java.util.function.BiConsumer<SendMessageResponse, Throwable> {
	public static final Logger logger = LoggerFactory.getLogger(SQSCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private MaxwellContext context;

	public SQSCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
			MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.context = context;
	}

	@Override
	public void accept(SendMessageResponse response, Throwable t) {
		if (t != null) {
			logger.error(t.getClass().getSimpleName() + " @ " + position + " -- ");
			logger.error(t.getLocalizedMessage());
			logger.error("Exception during put", t);

			if (!context.getConfig().ignoreProducerError) {
				context.terminate(new RuntimeException(t));
			} else {
				cc.markCompleted();
			}
			return;
		}

		if (logger.isDebugEnabled()) {
			logger.debug("-> Message id:{}, sequence number:{}  {}  {}",
					response.messageId(), response.sequenceNumber(), json, position);
		}
		cc.markCompleted();
	}

}
