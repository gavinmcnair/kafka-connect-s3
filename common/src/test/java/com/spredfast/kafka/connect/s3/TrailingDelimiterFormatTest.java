package com.spredfast.kafka.connect.s3;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;

import static com.spredfast.kafka.connect.s3.FormatTests.assertBytesAreEqual;

public class TrailingDelimiterFormatTest {

	@Test
	public void defaults() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withNullKeys(givenFormatWithConfig(ImmutableMap.of()), ImmutableList.of(
			FormatTests.Record.valueOnly("abcd"),
			FormatTests.Record.valueOnly("567\tav"),
			FormatTests.Record.valueOnly("2384732109847123098471092384723109847239084732409854329865293847549837")
		));
	}

	@Test
	public void withKeys() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withKeys(givenFormatWithConfig(ImmutableMap.of(
			"key.delimiter", "\t"
		)), ImmutableList.of(
			FormatTests.Record.keysAndValueOnly("123", "456"),
			FormatTests.Record.keysAndValueOnly("abc\ndef", "ghi\tjkl")
		), 0);
	}

	@Test
	public void withKeysAndHeaders() throws IOException {
		FormatTests.roundTrip_singlePartition_fromZero_withKeysAndHeaders(givenFormatWithConfig(ImmutableMap.of("key.delimiter", "\t")),
			ImmutableList.of(
				new FormatTests.Record("k1", "abcd", new RecordHeaders()),
				new FormatTests.Record("k2", "567av", new RecordHeaders(new RecordHeader[]{
					new RecordHeader("h1", "".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("h2", (byte[]) null),
					new RecordHeader("h3", "foo".getBytes(StandardCharsets.UTF_8)),
					new RecordHeader("h4", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)),
				})),
				new FormatTests.Record("k3", "23847321098471230984710923847231098472390847324098543298652938475", new RecordHeaders(new RecordHeader[]{
					new RecordHeader("h1", "foo".getBytes(StandardCharsets.UTF_8)),
				}))
			),
			0);
	}

	@Test
	public void outputWithKeys() {
		// using UTF_16BE because UTF_16 adds a byte order mark to every invocation of getBytes, which is annoying
		TrailingDelimiterFormat format = givenFormatWithConfig(ImmutableMap.of(
			"key.delimiter", "\t",
			"key.encoding", "UTF-16BE",
			"value.encoding", "UTF-16BE",
			"header.encoding", "UTF-16BE"
		));
		byte[] keyAndValue = "abc\tdef\n".getBytes(Charsets.UTF_16BE);
		byte[] headerDelimiter = new byte[]{0, 11};
		byte[] expected = new byte[keyAndValue.length + headerDelimiter.length];
		System.arraycopy(keyAndValue, 0, expected, 0, keyAndValue.length);
		System.arraycopy(headerDelimiter, 0, expected, keyAndValue.length, headerDelimiter.length);

		assertBytesAreEqual(expected, format.newWriter().writeBatch(Stream.of(
			new ProducerRecord<>("topic", "abc".getBytes(Charsets.UTF_16BE), "def".getBytes(Charsets.UTF_16BE))
		)).findFirst().get());
	}

	@Test
	public void outputWithKeysAndHeaders() {
		// using UTF_16BE because UTF_16 adds a byte order mark to every invocation of getBytes, which is annoying
		TrailingDelimiterFormat format = givenFormatWithConfig(ImmutableMap.of(
			"key.delimiter", "\t",
			"key.encoding", "UTF-16BE",
			"value.encoding", "UTF-16BE",
			"header.encoding", "UTF-16BE"
		));
		byte[] keyAndValue = "abc\tdef\n".getBytes(Charsets.UTF_16BE);
		byte[] headers = "[{\"key\":\"h1\",\"value\":[0,102,0,111,0,111]}]".getBytes(Charsets.UTF_8);
		byte[] headerDelimiter = new byte[]{0, 11};
		byte[] expected = new byte[keyAndValue.length + headers.length + headerDelimiter.length];
		System.arraycopy(keyAndValue, 0, expected, 0, keyAndValue.length);
		System.arraycopy(headers, 0, expected, keyAndValue.length, headers.length);
		System.arraycopy(headerDelimiter, 0, expected, keyAndValue.length + headers.length, headerDelimiter.length);

		assertBytesAreEqual(expected, format.newWriter().writeBatch(Stream.of(
			new ProducerRecord<>(
				"topic", null, "abc".getBytes(Charsets.UTF_16BE), "def".getBytes(Charsets.UTF_16BE),
				new RecordHeaders(new RecordHeader[]{new RecordHeader("h1", "foo".getBytes(StandardCharsets.UTF_16BE))})
			)
		)).findFirst().get());
	}

	private TrailingDelimiterFormat givenFormatWithConfig(ImmutableMap<String, Object> configs) {
		TrailingDelimiterFormat format = new TrailingDelimiterFormat();
		format.configure(configs);
		return format;
	}

}
