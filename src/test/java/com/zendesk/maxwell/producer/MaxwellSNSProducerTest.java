package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.NoOpMetrics;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class MaxwellSNSProducerTest {

	private static final long TIMESTAMP_MILLISECONDS = 1496712943447L;
	private static final String TOPIC = "topic";
	private static final String FIFO_TOPIC = "topic.fifo";
	private static final Position POSITION = new Position(new BinlogPosition(1L, "binlog-0001"), 0L);
	private static final CompletableFuture<PublishResponse> mockedFuture =
			CompletableFuture.completedFuture(PublishResponse.builder().messageId("mid").build());

	@Rule
	public final EnvironmentVariables environmentVariables
			= new EnvironmentVariables();

	@Captor ArgumentCaptor<PublishRequest> arguments;
	RowMap rowMap = new RowMap("insert", "MyDatabase", "MyTable", TIMESTAMP_MILLISECONDS, new ArrayList<String>(), POSITION);

	@Before
	public void beforeEach() {
		environmentVariables.set("AWS_ACCESS_KEY_ID", "AKIACCESSKEY");
		environmentVariables.set("AWS_SECRET_ACCESS_KEY", "SUPERSECRETKEY");
		environmentVariables.set("AWS_REGION", "us-west-2");
		environmentVariables.set("AWS_EC2_METADATA_DISABLED", "true");
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void publishesRecord() throws Exception {
		SnsAsyncClient client = Mockito.mock(SnsAsyncClient.class);
		MaxwellContext context = mock(MaxwellContext.class);
		when(context.getConfig()).thenReturn(new MaxwellConfig());
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		MaxwellSNSProducer producer = new MaxwellSNSProducer(context, TOPIC, "", "");
		producer.setClient(client);
		String payload = rowMap.toJSON();
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		when(client.publish(any(PublishRequest.class))).thenReturn(mockedFuture);
		producer.sendAsync(rowMap, cc);
		Mockito.verify(client, times(1)).publish(arguments.capture());
		Assert.assertEquals(arguments.getValue().topicArn(), TOPIC);
		Assert.assertEquals(arguments.getValue().message(), payload);
	}

	@Test
	public void setsMessageAttributes() throws Exception {
		SnsAsyncClient client = Mockito.mock(SnsAsyncClient.class);
		MaxwellContext context = mock(MaxwellContext.class);
		MaxwellConfig config = new MaxwellConfig();
		config.snsAttrs = "database,table";
		when(context.getConfig()).thenReturn(config);
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		MaxwellSNSProducer producer = new MaxwellSNSProducer(context, TOPIC, "", "");
		producer.setClient(client);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		when(client.publish(any(PublishRequest.class))).thenReturn(mockedFuture);
		producer.sendAsync(rowMap, cc);
		Mockito.verify(client, times(1)).publish(arguments.capture());
		Map<String, MessageAttributeValue> attributes = arguments.getValue().messageAttributes();
		Assert.assertNotNull(attributes.getOrDefault("table", null));
		Assert.assertEquals("MyTable", attributes.get("table").stringValue());
		Assert.assertNotNull(attributes.getOrDefault("database", null));
		Assert.assertEquals("MyDatabase", attributes.get("database").stringValue());
	}

	@Test
	public void ensureMessageGroupIdOnFifo() throws Exception {
		SnsAsyncClient client = Mockito.mock(SnsAsyncClient.class);
		MaxwellContext context = mock(MaxwellContext.class);
		when(context.getConfig()).thenReturn(new MaxwellConfig());
		when(context.getMetrics()).thenReturn(new NoOpMetrics());
		MaxwellSNSProducer producer = new MaxwellSNSProducer(context, FIFO_TOPIC, "", "");
		producer.setClient(client);
		AbstractAsyncProducer.CallbackCompleter cc = mock(AbstractAsyncProducer.CallbackCompleter.class);
		when(client.publish(any(PublishRequest.class))).thenReturn(mockedFuture);
		producer.sendAsync(rowMap, cc);
		Mockito.verify(client, times(1)).publish(arguments.capture());
		Assert.assertEquals("MyDatabase", arguments.getValue().messageGroupId());
	}
}
