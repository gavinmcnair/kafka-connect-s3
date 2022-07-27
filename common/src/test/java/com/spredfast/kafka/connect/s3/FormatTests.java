package com.spredfast.kafka.connect.s3;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class FormatTests {

	public static class Record {
		String key;
		String value;
		Headers headers;

		public Record(final String key, final String value, final Headers headers) {
			this.key = key;
			this.value = value;
			this.headers = headers;
		}

		public static Record keysAndValueOnly(final String key, final String value) {
			return new Record(key, value, null);
		}

		public static Record valueOnly(final String value) {
			return new Record(null, value, null);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			final Record record = (Record) o;
			return Objects.equals(key, record.key) && Objects.equals(value, record.value) && Objects.equals(headers, record.headers);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, value, headers);
		}

		@Override
		public String toString() {
			return "Record{" +
				"key='" + key + '\'' +
				", value='" + value + '\'' +
				", headers=" + headers +
				'}';
		}
	}

	public static void roundTrip_singlePartition_fromZero_withNullKeys(S3RecordFormat format, List<Record> values) throws IOException {
		roundTrip_singlePartition_fromZero_withNullKeys(format, values, 0L);
	}

	// don't currently have a format that cares about start offset, so overload isn't used
	public static void roundTrip_singlePartition_fromZero_withNullKeys(S3RecordFormat format, List<Record> values, long startOffset) throws IOException {
		Stream<ProducerRecord<byte[], byte[]>> records = values.stream().map(r -> r.value).map(v ->
			new ProducerRecord<>("topic", 0, null, v.getBytes())
		);

		List<Record> results = roundTrip(format, startOffset, records).stream().map(r -> Record.valueOnly(new String(r.value()))).collect(Collectors.toList());

		assertEquals(values, results);
	}

	public static void roundTrip_singlePartition_fromZero_withKeys(S3RecordFormat format, List<Record> records, long startOffset) throws IOException {
		Stream<ProducerRecord<byte[], byte[]>> producerRecords = records.stream().map(entry ->
			new ProducerRecord<>("topic", 0, entry.key.getBytes(), entry.value.getBytes())
		);

		List<Record> results = roundTrip(format, startOffset, producerRecords).stream().map(r -> Record.keysAndValueOnly(new String(r.key()), new String(r.value()))).collect(Collectors.toList());

		assertEquals(records, results);
	}

	public static void roundTrip_singlePartition_fromZero_withKeysAndHeaders(S3RecordFormat format, List<Record> records, long startOffset) throws IOException {
		Stream<ProducerRecord<byte[], byte[]>> producerRecords = records.stream().map(entry ->
			new ProducerRecord<>("topic", 0, entry.key.getBytes(), entry.value.getBytes(), entry.headers)
		);

		List<Record> results = roundTrip(format, startOffset, producerRecords).stream().map(r -> new Record(new String(r.key()), new String(r.value()), r.headers())).collect(Collectors.toList());

		assertEquals(records, results);
	}

	public static void assertBytesAreEqual(byte[] expected, byte[] actual) {
		if (!Arrays.equals(expected, actual)) {
			assertEquals(Base64.getEncoder().encodeToString(expected), Base64.getEncoder().encodeToString(actual));
		}
	}

	private static List<ConsumerRecord<byte[], byte[]>> roundTrip(S3RecordFormat format, long startOffset, Stream<ProducerRecord<byte[], byte[]>> records) throws IOException {
		S3RecordsWriter writer = format.newWriter();

		ByteArrayOutputStream boas = new ByteArrayOutputStream();
		boas.write(writer.init("topic", 0, startOffset));

		writer.writeBatch(records).forEach(b -> boas.write(b, 0, b.length));
		boas.write(writer.finish("topic", 0));

		ByteArrayInputStream in = new ByteArrayInputStream(boas.toByteArray());
		S3RecordsReader reader = format.newReader();
		if (reader.isInitRequired()) {
			reader.init("topic", 0, in, startOffset);
		}
		List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
		reader.readAll("topic", 0, in, startOffset).forEachRemaining(results::add);
		return results;
	}


}
