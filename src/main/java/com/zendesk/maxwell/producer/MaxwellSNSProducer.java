package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.partitioners.MaxwellSNSPartitioner;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MaxwellSNSProducer extends AbstractAsyncProducer {

	private SnsAsyncClient client;
	private String topic;
	private MaxwellSNSPartitioner partitioner;

	public MaxwellSNSProducer(MaxwellContext context, String topic, String serviceEndpoint, String signingRegion) {
		super(context);
		this.topic = topic;

		// Only configure custom endpoint if both serviceEndpoint and signingRegion are provided
		if (serviceEndpoint != null && !serviceEndpoint.trim().isEmpty() &&
			signingRegion != null && !signingRegion.trim().isEmpty()) {
			this.client = SnsAsyncClient.builder()
					.endpointOverride(URI.create(serviceEndpoint))
					.region(Region.of(signingRegion))
					.build();
		} else {
			// Use default client configuration when endpoint parameters are not provided
			this.client = SnsAsyncClient.create();
		}
		String partitionKey = context.getConfig().producerPartitionKey;
		String partitionColumns = context.getConfig().producerPartitionColumns;
		String partitionFallback = context.getConfig().producerPartitionFallback;
		this.partitioner = new MaxwellSNSPartitioner(partitionKey, partitionColumns, partitionFallback);
	}

	public void setClient(SnsAsyncClient client) {
		this.client = client;
	}

	@Override
	public void sendAsync(RowMap r, CallbackCompleter cc) throws Exception {
		String value = r.toJSON(outputConfig);

		Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
		final String configuredAttributes = context.getConfig().snsAttrs;
		if (configuredAttributes != null) {
			for (String element: configuredAttributes.split(",")) {
				switch (element) {
					case "database":
						messageAttributes.put(
							"database",
							MessageAttributeValue.builder().dataType("String").stringValue(r.getDatabase()).build()
						);
						break;
					case "table":
						messageAttributes.put(
							"table",
							MessageAttributeValue.builder().dataType("String").stringValue(r.getTable()).build()
						);
						break;
				}
			}
		}

		PublishRequest.Builder builder = PublishRequest.builder()
				.topicArn(topic)
				.message(value)
				.messageAttributes(messageAttributes);

		if (topic.endsWith(".fifo")) {
			String key = this.partitioner.getSNSKey(r);
			builder.messageGroupId(key);
		}

		PublishRequest publishRequest = builder.build();

		SNSCallback callback = new SNSCallback(cc, r.getNextPosition(), value,
			r.getDatabase(), r.getTable(), r.getRowIdentity().toConcatString(), r.getApproximateSize(), context);
		client.publish(publishRequest).whenComplete(callback);
	}

}

class SNSCallback implements java.util.function.BiConsumer<PublishResponse, Throwable> {
	public static final Logger logger = LoggerFactory.getLogger(SNSCallback.class);

	private final AbstractAsyncProducer.CallbackCompleter cc;
	private final Position position;
	private final String json;
	private final String database;
	private final String table;
	private final String pk;
	private final long approximateRowSize;
	private MaxwellContext context;

	public SNSCallback(AbstractAsyncProducer.CallbackCompleter cc, Position position, String json,
			String database, String table, String pk, long approximateRowSize,
			MaxwellContext context) {
		this.cc = cc;
		this.position = position;
		this.json = json;
		this.context = context;
		this.database = database;
		this.table = table;
		this.pk = pk;
		this.approximateRowSize = approximateRowSize;
	}

	@Override
	public void accept(PublishResponse response, Throwable t) {
		if (t != null) {
			logger.error(t.getClass().getSimpleName() + " @ " + position + " -- ");
			logger.error("Database:" + database + ", Table:" + table + ", PK:" + pk + ", Approx Size:" + Long.toString(approximateRowSize));
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
			logger.debug("-> MessageId: {}", response.messageId());
		}
		cc.markCompleted();
	}

}
