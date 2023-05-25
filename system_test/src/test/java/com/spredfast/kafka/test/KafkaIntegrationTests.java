package com.spredfast.kafka.test;

import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.netflix.curator.test.InstanceSpec;
import com.netflix.curator.test.TestingServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.metadata.BrokerState;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaIntegrationTests {

	private static final int SLEEP_INTERVAL = 300;

	public static Kafka givenLocalKafka() throws Exception {
		return new Kafka();
	}

	public static void waitForPassing(Duration timeout, Runnable test) {
		waitForPassing(timeout, () -> {
			test.run();
			return null;
		});
	}

	public static <T> T waitForPassing(Duration timeout, Callable<T> test) {
		AssertionError last = null;
		for (int i = 0; i < timeout.toMillis() / SLEEP_INTERVAL; i++) {
			try {
				return test.call();
			} catch (AssertionError e) {
				last = e;
				try {
					Thread.sleep(SLEEP_INTERVAL);
				} catch (InterruptedException e1) {
					Throwables.propagate(e1);
				}
			} catch (Exception e) {
				Throwables.propagate(e);
			}
		}
		if (last != null) {
			throw last;
		}
		return null;
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort) throws IOException {
		return givenKafkaConnect(kafkaPort, ImmutableMap.of());
	}

	public static KafkaConnect givenKafkaConnect(int kafkaPort, Map<? extends String, ? extends String> overrides) throws IOException {
		File tempFile = File.createTempFile("connect", "offsets");
		System.err.println("Storing offsets at " + tempFile);
		HashMap<String, String> props = new HashMap<>(ImmutableMap.<String, String>builder()
			.put("bootstrap.servers", "localhost:" + kafkaPort)
			// perform no conversion
			.put("key.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("value.converter", "com.spredfast.kafka.connect.s3.AlreadyBytesConverter")
			.put("internal.key.converter", QuietJsonConverter.class.getName())
			.put("internal.value.converter", QuietJsonConverter.class.getName())
			.put("internal.key.converter.schemas.enable", "true")
			.put("internal.value.converter.schemas.enable", "true")
			.put("offset.storage.file.filename", tempFile.getCanonicalPath())
			.put("offset.flush.interval.ms", "1000")
			.put("consumer.metadata.max.age.ms", "1000")
			.put("listeners", "http://localhost:" + InstanceSpec.getRandomPort())
			.build()
		);
		props.putAll(overrides);

		return givenKafkaConnect(props);
	}

	public static AdminClient givenAnAdminClient(int kafkaPort) {
		return AdminClient.create(
			new HashMap<>(ImmutableMap.<String, String>builder()
				.put("bootstrap.servers", "localhost:" + kafkaPort)
				.build())
		);
	}

	private static KafkaConnect givenKafkaConnect(Map<String, String> props) {
		WorkerConfig config = new StandaloneConfig(props);
		Plugins plugins = new Plugins(props);
		plugins.compareAndSwapWithDelegatingLoader();
		ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
			config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
			config, ConnectorClientConfigOverridePolicy.class);

		String workerId = props.get("bootstrap.servers");
		Worker worker = new Worker(workerId, new SystemTime(), plugins, config, new FileOffsetBackingStore(),
			connectorClientConfigOverridePolicy);
		Herder herder = new StandaloneHerder(worker, ConnectUtils.lookupKafkaClusterId(config), connectorClientConfigOverridePolicy);


		RestServer restServer = new RestServer(config);
		restServer.initializeServer();
		Connect connect = new Connect(herder, restServer);
		connect.start();
		return new KafkaConnect(connect, herder, () -> givenKafkaConnect(props));
	}

	public static class KafkaConnect implements AutoCloseable {

		private final Connect connect;
		private final Herder herder;
		private final Supplier<KafkaConnect> restart;

		public KafkaConnect(Connect connect, Herder herder, Supplier<KafkaConnect> restart) {
			this.connect = connect;
			this.herder = herder;
			this.restart = restart;
		}

		@Override
		public void close() throws Exception {
			connect.stop();
			connect.awaitStop();
		}

		public KafkaConnect restart() {
			return restart.get();
		}

		public Herder herder() {
			return herder;
		}
	}

	public static class Kafka implements AutoCloseable {
		private final TestingServer zk;
		private final KafkaServer kafkaServer;

		private final int localPort;

		public Kafka() throws Exception {
			zk = new TestingServer();
			localPort = InstanceSpec.getRandomPort();
			File tmpDir = Files.createTempDir();
			KafkaConfig config = new KafkaConfig(Maps.transformValues(ImmutableMap.<String, Object>builder()
				.put("listeners", "PLAINTEXT://localhost:" + localPort)
				.put("advertised.listeners", "PLAINTEXT://localhost:" + localPort)
				.put("port", localPort)
				.put("broker.id", "1")
				.put("offsets.topic.replication.factor", 1)
				.put("log.dir", tmpDir.getCanonicalPath())
				.put("zookeeper.connect", zk.getConnectString())
				.build(), Functions.toStringFunction()));
			kafkaServer = new KafkaServer(config, Time.SYSTEM, Option.empty(), true);
			kafkaServer.startup();
		}

		public int getLocalPort() {
			return localPort;
		}

		@Override
		public void close() throws Exception {
			kafkaServer.shutdown();
			kafkaServer.awaitShutdown();
			zk.close();
		}

		public String createUniqueTopic(String prefix) throws InterruptedException {
			return createUniqueTopic(prefix, 1);
		}

		public String createUniqueTopic(String prefix, int partitions) throws InterruptedException {
			return createUniqueTopic(prefix, partitions, new HashMap<>());
		}

		public String createUniqueTopic(String prefix, int partitions, Map<String, String> topicConfig) {
			checkReady();

			String topic = (prefix + UUID.randomUUID().toString().substring(0, 5)).replaceAll("[^a-zA-Z0-9._-]", "_");
			short replicationFactor = 1;
			List<NewTopic> topics = new ArrayList<>();
			topics.add(new NewTopic(topic, partitions, replicationFactor));

			try (AdminClient admin = givenAnAdminClient(localPort)) {
				admin.createTopics(topics);
			}
			updateTopic(topic, topicConfig);

			return topic;
		}

		public void updateTopic(String topic, Map<String, String> topicConfig) {
			List<AlterConfigOp> topicAlterations = topicConfig.entrySet().stream()
				.map((entry) -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
				.collect(Collectors.toList());

			Map<ConfigResource, Collection<AlterConfigOp>> alterations = new HashMap<>();
			alterations.put(new ConfigResource(ConfigResource.Type.TOPIC, topic), topicAlterations);

			try (AdminClient admin = givenAnAdminClient(localPort)) {
				admin.incrementalAlterConfigs(alterations);
			}
		}

		public void checkReady() {
			checkReady(Duration.ofSeconds(15));
		}

		public void checkReady(Duration timeout) {
			waitForPassing(timeout, () -> assertEquals(BrokerState.RUNNING, kafkaServer.brokerState()));
		}
	}

}
