package com.spredfast.kafka.connect.s3;


import com.amazonaws.services.s3.AmazonS3;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;

public class S3ClientTest {
	@Rule
	public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

	@Test
	public void shouldConstructClient() {
		// set the AWS region for test purposes
		environmentVariables.set("AWS_REGION", "eu-west-2");
		Map<String, String> config = new HashMap<>();
		config.put("s3.endpoint", "https://s3-eu-west-2.amazonaws.com");
		AmazonS3 client = S3.s3client(config);
		assertThat(client, not(nullValue()));
	}
}
