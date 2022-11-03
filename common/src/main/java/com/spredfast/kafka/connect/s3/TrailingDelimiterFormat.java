package com.spredfast.kafka.connect.s3;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Reads/writes records to/from a delimited, encoded string.
 * Both delimiter and encoding are configurable.
 */
public class TrailingDelimiterFormat implements S3RecordFormat, Configurable {

	public static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;
	public static final String DEFAULT_DELIMITER = "\n";
	private static final byte[] NO_BYTES = {};
	private static final Gson GSON = new Gson();
	private static final String HEADER_DELIMITER = new String(new byte[]{11});
	private byte[] valueDelimiter;
	private byte[] headerDelimiter;
	private Optional<byte[]> keyDelimiter;

	@Override
	public void configure(Map<String, ?> configs) {
		valueDelimiter = Optional.ofNullable(configs.get("value.delimiter"))
			.map(Object::toString)
			.orElse(DEFAULT_DELIMITER)
			.getBytes(parseEncoding(configs, "value.encoding"));

		keyDelimiter = Optional.ofNullable(configs.get("key.delimiter"))
			.map(Object::toString)
			.map(s -> s.getBytes(parseEncoding(configs, "key.encoding")));

		headerDelimiter = Optional.ofNullable(configs.get("header.delimiter"))
			.map(Object::toString)
			.orElse(HEADER_DELIMITER)
			.getBytes(parseEncoding(configs, "header.encoding"));

		if (!keyDelimiter.isPresent() && configs.containsKey("key.encoding")) {
			throw new IllegalArgumentException("Key encoding specified without delimiter!");
		}
	}

	private Charset parseEncoding(Map<String, ?> configs, String key) {
		return Optional.ofNullable(configs.get(key))
			.map(Object::toString)
			.map(Charset::forName)
			.orElse(DEFAULT_ENCODING);
	}

	private byte[] encode(ProducerRecord<byte[], byte[]> record) {
		Optional<byte[]> headers = serialiseHeaders(record.headers());

		List<byte[]> bytes = Stream.of(
			Optional.ofNullable(record.key()).filter(r -> keyDelimiter.isPresent()),
			keyDelimiter,
			Optional.ofNullable(record.value()),
			Optional.of(valueDelimiter),
			headers,
			Optional.of(headerDelimiter) // include header delimiter even if there are no headers so it's easier to parse
		).map(o -> o.orElse(NO_BYTES)).filter(arr -> arr.length > 0).collect(toList());
		int size = bytes.stream().map(arr -> arr.length).reduce(0, (a, b) -> a + b);
		byte[] result = new byte[size];
		for (int i = 0, written = 0; i < bytes.size(); i++) {
			byte[] src = bytes.get(i);
			System.arraycopy(src, 0, result, written, src.length);
			written += src.length;
		}
		return result;
	}

	private Optional<byte[]> serialiseHeaders(Headers headers) {
		return Optional.ofNullable(headers)
			.map(Headers::toArray)
			.filter(a -> a.length > 0)
			.map(GSON::toJson)
			.map(a -> a.getBytes(DEFAULT_ENCODING));
	}

	@Override
	public S3RecordsWriter newWriter() {
		return records -> records.map(this::encode);
	}

	@Override
	public S3RecordsReader newReader() {
		return new DelimitedRecordReader(valueDelimiter, keyDelimiter, headerDelimiter);
	}

}
