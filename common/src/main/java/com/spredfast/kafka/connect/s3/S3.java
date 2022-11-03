package com.spredfast.kafka.connect.s3;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.util.Map;
import java.util.Objects;

import static com.amazonaws.services.s3.AmazonS3Client.S3_SERVICE_NAME;
import static com.amazonaws.util.AwsHostNameUtils.parseRegion;
import static java.lang.Boolean.parseBoolean;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		// Use default credentials provider that looks in Env + Java properties + profile + instance role
		AmazonS3ClientBuilder s3Client = AmazonS3ClientBuilder.standard();

		// If worker config sets explicit endpoint override (e.g. for testing) use that
		setS3Endpoint(config, s3Client);
		if (parseBoolean(config.get("s3.path_style"))) {
			s3Client.setPathStyleAccessEnabled(true);
		}
		return s3Client.build();
	}

	private static void setS3Endpoint(Map<String, String> config, AmazonS3ClientBuilder s3Client) {
		String s3Endpoint = config.get("s3.endpoint");
		if (s3Endpoint != null && !Objects.equals(s3Endpoint, "")) {
			s3Client.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
				s3Endpoint, parseRegion(s3Endpoint, S3_SERVICE_NAME)
			));
		}
	}

}
