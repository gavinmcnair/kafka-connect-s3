package com.spredfast.kafka.connect.s3;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class FakeS3 {

	@Container
	public GenericContainer s3 = new GenericContainer(DockerImageName.parse("adobe/s3mock"))
		.withExposedPorts(9090);

	public void start() {
		s3.start();
	}

	public void close() {
		s3.close();
	}

	public String getEndpoint() {
		return String.format("http://%s:%s", s3.getHost(), s3.getFirstMappedPort());
	}
}
