package com.spredfast.kafka.connect.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.spredfast.kafka.connect.s3.sink.BlockGZIPFileWriter;
import com.spredfast.kafka.connect.s3.source.S3FilesReader;
import com.spredfast.kafka.connect.s3.source.S3Offset;
import com.spredfast.kafka.connect.s3.source.S3Partition;
import com.spredfast.kafka.connect.s3.source.S3SourceConfig;
import com.spredfast.kafka.connect.s3.source.S3SourceRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers S3 and reading raw byte records. Closer to an integration test.
 */
class S3FilesReaderTest {
	private final FakeS3 s3 = new FakeS3();
	private String bucketName = "bucket";
	private static final Random RANDOM = new Random();

	private static final BiFunction<String, String, Headers> headersFunction = (k, v) -> new RecordHeaders(new RecordHeader[]{new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8))});


	@BeforeEach
	void setUp() {
		bucketName = String.format("test-bucket-%d", RANDOM.nextLong());
		s3.start();
	}

	@AfterEach
	void tearDown() {
		s3.close();
	}

	@Test
	void testReadingBytesFromS3() throws IOException {
		final AmazonS3 client = s3Client();
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(client, true, 3);

		thenTheyAreInOrder(results);
	}

	@Test
	void testReadingBytesFromS3_multiPartition() throws IOException {
		// scenario: multiple partition files at the end of a listing, page size >  # of files
		// do we read all of them?
		final AmazonS3 client = s3Client();
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenASingleDayWithManyPartitions(client, dir);

		List<String> results = whenTheRecordsAreRead(client, true, 10);

		thenTheyAreInOrder(results);
	}

	@Test
	void testReadingBytesFromS3_withOffsets() throws IOException {
		final AmazonS3 client = s3Client();
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/2015-12-31/topic-00003-000000000001.gz", 5L, "00003"));

		assertEquals(Arrays.asList(
			"willbe=skipped5[skipped header key5:skipped header value5]",
			"willbe=skipped6[skipped header key6:skipped header value6]",
			"willbe=skipped7[skipped header key7:skipped header value7]",
			"willbe=skipped8[skipped header key8:skipped header value8]",
			"willbe=skipped9[skipped header key9:skipped header value9]"
		), results);
	}


	@Test
	void testReadingBytesFromS3_withOffsetsAtEndOfFile() throws IOException {
		final AmazonS3 client = s3Client();
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeDataWithKeys(client, dir);

		// this file will be skipped
		List<String> results = whenTheRecordsAreRead(givenAReaderWithOffsets(client,
			"prefix/2015-12-30/topic-00003-000000000000.gz", 1L, "00003"));

		assertEquals(
			IntStream.range(1, 10)
				.mapToObj(i -> String.format("willbe=skipped%d[skipped header key%d:skipped header value%d]", i, i, i))
				.collect(toList()),
			results);
	}

	S3FilesReader givenAReaderWithOffsets(AmazonS3 client, String marker, long nextOffset, final String partition) {
		Map<S3Partition, S3Offset> offsets = new HashMap<>();
		int partInt = Integer.valueOf(partition, 10);
		offsets.put(S3Partition.from(bucketName, "prefix", "topic", partInt),
			S3Offset.from(marker, nextOffset - 1 /* an S3 offset is the last record processed, so go back 1 to consume next */));
		return new S3FilesReader(new S3SourceConfig(bucketName, "prefix", 1, null, S3FilesReader.DEFAULT_PATTERN, S3FilesReader.InputFilter.GUNZIP,
			p -> partInt == p), client, offsets, () -> new BytesRecordReader(true));
	}

	static class ReversedStringBytesConverter implements Converter {
		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			// while we're here, verify that we get our subconfig
			assertEquals("isPresent", configs.get("requiredProp"));
		}

		@Override
		public byte[] fromConnectData(String topic, Schema schema, Object value) {
			byte[] bytes = value.toString().getBytes(Charset.forName("UTF-8"));
			byte[] result = new byte[bytes.length];
			for (int i = 0; i < bytes.length; i++) {
				result[bytes.length - i - 1] = bytes[i];
			}
			return result;
		}

		@Override
		public SchemaAndValue toConnectData(String topic, byte[] value) {
			throw new UnsupportedOperationException();
		}
	}

	@Test
	void testReadingBytesFromS3_withoutKeys() throws IOException {
		final AmazonS3 client = s3Client();
		final Path dir = Files.createTempDirectory("s3FilesReaderTest");
		givenSomeDataWithoutKeys(client, dir);

		List<String> results = whenTheRecordsAreRead(client, false);

		theTheyAreInOrderWithoutKeys(results);
	}

	Converter givenACustomConverter() {
		Map<String, Object> config = new HashMap<>();
		config.put("converter", AlreadyBytesConverter.class.getName());
		config.put("converter.converter", ReversedStringBytesConverter.class.getName());
		config.put("converter.converter.requiredProp", "isPresent");
		return Configure.buildConverter(config, "converter", false, null);
	}

	void theTheyAreInOrderWithoutKeys(List<String> results) {
		List<String> expected = Arrays.asList(
			"value0-0[header key 0-0:header value 0-0]",
			"value1-0[header key 1-0:header value 1-0]",
			"value1-1[header key 1-1:header value 1-1]"
		);
		assertEquals(expected, results);
	}

	private void thenTheyAreInOrder(List<String> results) {
		List<String> expected = Arrays.asList(
			"key0-0=value0-0[header key 0-0:header value 0-0]",
			"key1-0=value1-0[header key 1-0:header value 1-0]",
			"key1-1=value1-1[header key 1-1:header value 1-1]"
		);
		assertEquals(expected, results);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client, boolean fileIncludesKeys) {
		return whenTheRecordsAreRead(client, fileIncludesKeys, 1);
	}

	private List<String> whenTheRecordsAreRead(AmazonS3 client, boolean fileIncludesKeys, int pageSize) {
		S3FilesReader reader = new S3FilesReader(new S3SourceConfig(bucketName, "prefix", pageSize, "prefix/2016-01-01", S3FilesReader.DEFAULT_PATTERN, S3FilesReader.InputFilter.GUNZIP, null), client, null, () -> new BytesRecordReader(fileIncludesKeys));
		return whenTheRecordsAreRead(reader);
	}

	private List<String> whenTheRecordsAreRead(S3FilesReader reader) {
		List<String> results = new ArrayList<>();
		for (S3SourceRecord record : reader) {
			String key = (record.key() == null ? "" : new String(record.key()) + "=");
			String value = new String(record.value());
			String headers = StreamSupport.stream(record.headers().spliterator(), false)
				.map(h -> h.key() + ":" + (String) h.value())
				.collect(Collectors.joining(","));

			results.add(key + value + "[" + headers + "]");
		}
		return results;
	}

	private void givenASingleDayWithManyPartitions(AmazonS3 client, Path dir) throws IOException {
		givenASingleDayWithManyPartitions(client, dir, true);
	}

	private void givenASingleDayWithManyPartitions(AmazonS3 client, Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		try (BlockGZIPFileWriter p0 = new BlockGZIPFileWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512);
			 BlockGZIPFileWriter p1 = new BlockGZIPFileWriter("topic-00001", dir.toString() + "/prefix/2016-01-01", 0, 512);
		) {
			write(p0, "key0-0".getBytes(), "value0-0".getBytes(), headersFunction.apply("header key 0-0", "header value 0-0"), includeKeys);
			write(p1, "key1-0".getBytes(), "value1-0".getBytes(), headersFunction.apply("header key 1-0", "header value 1-0"), includeKeys);
			write(p1, "key1-1".getBytes(), "value1-1".getBytes(), headersFunction.apply("header key 1-1", "header value 1-1"), includeKeys);
		}
		uploadToS3(client, dir);
	}

	private void givenSomeDataWithKeys(AmazonS3 client, Path dir) throws IOException {
		givenSomeData(client, dir, true);
	}

	private void givenSomeDataWithoutKeys(AmazonS3 client, Path dir) throws IOException {
		givenSomeData(client, dir, false);
	}

	private void givenSomeData(AmazonS3 client, Path dir, boolean includeKeys) throws IOException {
		new File(dir.toFile(), "prefix/2015-12-30").mkdirs();
		new File(dir.toFile(), "prefix/2015-12-31").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-01").mkdirs();
		new File(dir.toFile(), "prefix/2016-01-02").mkdirs();
		try (BlockGZIPFileWriter writer0 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-31", 1, 512);
			 BlockGZIPFileWriter writer1 = new BlockGZIPFileWriter("topic-00000", dir.toString() + "/prefix/2016-01-01", 0, 512);
			 BlockGZIPFileWriter writer2 = new BlockGZIPFileWriter("topic-00001", dir.toString() + "/prefix/2016-01-02", 0, 512);
			 BlockGZIPFileWriter preWriter1 = new BlockGZIPFileWriter("topic-00003", dir.toString() + "/prefix/2015-12-30", 0, 512);
		) {
			write(preWriter1, "willbe".getBytes(), "skipped0".getBytes(), headersFunction.apply("skipped header key", "skipped header value"), includeKeys);

			for (int i = 1; i < 10; i++) {
				write(writer0, "willbe".getBytes(), ("skipped" + i).getBytes(), headersFunction.apply("skipped header key" + i, "skipped header value" + i), includeKeys);
			}

			write(writer1, "key0-0".getBytes(), "value0-0".getBytes(), headersFunction.apply("header key 0-0", "header value 0-0"), includeKeys);

			write(writer2, "key1-0".getBytes(), "value1-0".getBytes(), headersFunction.apply("header key 1-0", "header value 1-0"), includeKeys);
			write(writer2, "key1-1".getBytes(), "value1-1".getBytes(), headersFunction.apply("header key 1-1", "header value 1-1"), includeKeys);
		}

		uploadToS3(client, dir);
	}

	private void uploadToS3(AmazonS3 client, Path dir) throws IOException {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(client).build();
		Files.walk(dir).filter(Files::isRegularFile).forEach(f -> {
			Path relative = dir.relativize(f);
			System.out.println("Writing " + relative.toString());
			Upload upload = tm.upload(bucketName, relative.toString(), f.toFile());
			try {
				upload.waitForUploadResult();
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		});
	}

	private void write(BlockGZIPFileWriter writer, byte[] key, byte[] value, Headers headers, boolean includeKeys) throws IOException {
		writer.write(new ByteLengthFormat(includeKeys).newWriter().writeBatch(Stream.of(
			new ProducerRecord<>("", 0, key, value, headers)
		)).collect(toList()), 1);
	}

	private AmazonS3 s3Client() {
		Map<String, String> config = ImmutableMap.of(
			"s3.new.record.poll.interval", "200",
			"s3.endpoint", s3.getEndpoint(),
			"s3.path_style", "true"  // necessary for FakeS3
		);
		AmazonS3 client = S3.s3client(config);
		client.createBucket(bucketName);
		return client;
	}


}
