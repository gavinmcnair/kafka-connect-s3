package com.spredfast.kafka.connect.s3;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NO_TIMESTAMP;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;

/**
 * Reads records that are followed by byte delimiters.
 */
public class DelimitedRecordReader implements RecordReader {
	private static final Gson GSON = new Gson();
	private final byte[] valueDelimiter;

	private final Optional<byte[]> keyDelimiter;

	private final byte[] headerDelimiter;

	public DelimitedRecordReader(byte[] valueDelimiter, Optional<byte[]> keyDelimiter, byte[] headerDelimiter) {
		this.valueDelimiter = valueDelimiter;
		this.keyDelimiter = keyDelimiter;
		this.headerDelimiter = headerDelimiter;
	}

	@Override
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException {
		Optional<byte[]> key = Optional.empty();
		if (keyDelimiter.isPresent()) {
			key = Optional.ofNullable(readTo(data, keyDelimiter.get()));
			if (!key.isPresent()) {
				return null;
			}
		}
		byte[] value = readTo(data, valueDelimiter);
		if (value == null) {
			if (key.isPresent()) {
				throw new IllegalStateException("missing value for key!" + new String(key.get()));
			}
			return null;
		}

		byte[] headersJson = readTo(data, headerDelimiter);
		Headers headers = deserialiseHeaders(headersJson);

		return new ConsumerRecord<>(
			topic, partition, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
			(long) NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, key.orElse(null), value, headers
		);
	}

	private Headers deserialiseHeaders(final byte[] headerBlock) {
		if (headerBlock == null || headerBlock.length == 0) {
			return new RecordHeaders();
		}

		String jsonString = new String(headerBlock);
		Type listType = new TypeToken<ArrayList<RecordHeader>>() {
		}.getType();
		List<Header> headers = GSON.fromJson(jsonString, listType);
		return new RecordHeaders(headers);
	}

	// read up to and including the given multi-byte delimeter
	private byte[] readTo(BufferedInputStream data, byte[] del) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int lastByte = del[del.length - 1] & 0xff;
		int b;
		while ((b = data.read()) != -1) {
			baos.write(b);
			if (b == lastByte && baos.size() >= del.length) {
				byte[] bytes = baos.toByteArray();
				if (endsWith(bytes, del)) {
					byte[] undelimited = new byte[bytes.length - del.length];
					System.arraycopy(bytes, 0, undelimited, 0, undelimited.length);
					return undelimited;
				}
			}
		}
		// if we got here, we got EOF before we got the delimiter
		return (baos.size() == 0) ? null : baos.toByteArray();
	}

	private boolean endsWith(byte[] bytes, byte[] suffix) {
		for (int i = 0; i < suffix.length; i++) {
			if (bytes[bytes.length - suffix.length + i] != suffix[i]) {
				return false;
			}
		}
		return true;
	}

	private static byte[] delimiterBytes(String value, String encoding) {
		return ofNullable(value).orElse(TrailingDelimiterFormat.DEFAULT_DELIMITER).getBytes(
			ofNullable(encoding).map(Charset::forName).orElse(TrailingDelimiterFormat.DEFAULT_ENCODING)
		);
	}

	public static RecordReader from(Map<String, String> taskConfig) {
		return new DelimitedRecordReader(
			delimiterBytes(taskConfig.get("value.converter.delimiter"), taskConfig.get("value.converter.encoding")),
			taskConfig.containsKey("key.converter")
				? Optional.of(delimiterBytes(taskConfig.get("key.converter.delimiter"), taskConfig.get("key.converter.encoding")))
				: Optional.empty(),
			delimiterBytes(taskConfig.get("header.converter.delimiter"), taskConfig.get("header.converter.encoding"))
		);
	}
}
