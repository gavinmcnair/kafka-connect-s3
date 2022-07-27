package com.spredfast.kafka.connect.s3;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static com.spredfast.kafka.connect.s3.Constants.HEADER_MARKER;
import static com.spredfast.kafka.connect.s3.Constants.HEADER_MARKER_SIZE;
import static com.spredfast.kafka.connect.s3.Constants.LENGTH_FIELD_SIZE;
import static com.spredfast.kafka.connect.s3.Constants.NO_BYTES;

/**
 * Encodes raw bytes, prefixed by a 4 byte, big-endian integer
 * indicating the length of the byte sequence.
 * <p>
 * If the record contains headers then the value block is followed by:
 * - a single byte of value 0xF6
 * - a 4 byte, big-endian integer indicating the length of the headers' byte sequence
 * - the byte array representation of the headers encoded os JSON
 * <p>
 * The value of the header marker (0xF6) is arbitrary.
 * It just needs to be a value that would not normally be at the start of the next record's 4 byte integer -- i.e. a negative value.
 * <p>
 * A more concise headers encoding was considered, such as the variable length encoding used for the message key and value, but care would be needed to differentiate nulls and empty strings.
 * JSON is easy and storage is cheap.
 */
public class ByteLengthFormat implements S3RecordFormat, Configurable {

	private static final Gson GSON = new Gson();

	private Optional<Boolean> includesKeys;

	public ByteLengthFormat() {
	}

	public ByteLengthFormat(boolean includesKeys) {
		this.includesKeys = includesKeys ? Optional.of(true) : Optional.empty();
	}

	@Override
	public void configure(Map<String, ?> configs) {
		includesKeys = Optional.ofNullable(configs.get("include.keys")).map(Object::toString)
			.map(Boolean::valueOf).filter(f -> f);
	}

	@Override
	public S3RecordsWriter newWriter() {
		return records -> records.map(this::encode);
	}

	private byte[] encode(ProducerRecord<byte[], byte[]> r) {
		// write optionally the key, and the value, each preceded by their length
		byte[] key = includesKeys.flatMap(t -> Optional.ofNullable(r.key())).orElse(NO_BYTES);
		byte[] value = Optional.ofNullable(r.value()).orElse(NO_BYTES);
		Optional<byte[]> headers = serialiseHeaders(r.headers());

		int keyBlockLength = includesKeys.map(t -> LENGTH_FIELD_SIZE + key.length).orElse(0);
		int valueBlockLength = LENGTH_FIELD_SIZE + value.length;
		int headerBlockLength = headers.map(t -> HEADER_MARKER_SIZE + LENGTH_FIELD_SIZE + t.length).orElse(0);
		byte[] result = new byte[keyBlockLength + valueBlockLength + headerBlockLength];
		ByteBuffer wrapped = ByteBuffer.wrap(result);

		includesKeys.ifPresent(t -> {
			wrapped.putInt(key.length);
			wrapped.put(key);
		});

		wrapped.putInt(value.length);
		wrapped.put(value);

		if (headers.isPresent()) {
			wrapped.put(HEADER_MARKER);
			wrapped.putInt(headers.get().length);
			wrapped.put(headers.get());
		}

		return result;
	}

	private Optional<byte[]> serialiseHeaders(Headers headers) {
		return Optional.ofNullable(headers)
			.map(Headers::toArray)
			.filter(a -> a.length > 0)
			.map(GSON::toJson)
			.map(a -> a.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public S3RecordsReader newReader() {
		return new BytesRecordReader(includesKeys.isPresent());
	}

}
