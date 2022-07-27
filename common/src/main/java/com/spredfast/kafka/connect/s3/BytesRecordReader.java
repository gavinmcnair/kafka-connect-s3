package com.spredfast.kafka.connect.s3;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.DataException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.spredfast.kafka.connect.s3.Constants.HEADER_MARKER;
import static com.spredfast.kafka.connect.s3.Constants.HEADER_MARKER_SIZE;
import static com.spredfast.kafka.connect.s3.Constants.LENGTH_FIELD_SIZE;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NO_TIMESTAMP;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;

/**
 * Helper for reading raw length encoded records from a chunk file. Not thread safe.
 */
public class BytesRecordReader implements RecordReader {
	private static final Gson GSON = new Gson();
	private final ByteBuffer lenBuffer = ByteBuffer.allocate(LENGTH_FIELD_SIZE);

	private final boolean includesKeys;

	/**
	 * @param includesKeys do the serialized records include keys? Or just values?
	 */
	public BytesRecordReader(boolean includesKeys) {
		this.includesKeys = includesKeys;
	}

	public static class ReadContext {
		String topic;
		int partition;
		long offset;
		BufferedInputStream data;

		public ReadContext(final String topic, final int partition, final long offset, final BufferedInputStream data) {
			this.topic = topic;
			this.partition = partition;
			this.offset = offset;
			this.data = data;
		}
	}

	/**
	 * Reads a record from the given uncompressed data stream.
	 *
	 * @return a raw ConsumerRecord or null if at the end of the data stream.
	 */
	@Override
	public ConsumerRecord<byte[], byte[]> read(String topic, int partition, long offset, BufferedInputStream data) throws IOException {
		ReadContext context = new ReadContext(topic, partition, offset, data);
		return read(context);
	}

	public ConsumerRecord<byte[], byte[]> read(final ReadContext context) throws IOException {
		final byte[] key;
		final int valSize;
		if (includesKeys) {
			// if at the end of the stream, return null
			final Integer keySize = readLen(context);
			if (keySize == null) {
				return null;
			}
			key = readBytes(keySize, context);
			valSize = readValueLen(context);
		} else {
			key = null;
			Integer vSize = readLen(context);
			if (vSize == null) {
				return null;
			}
			valSize = vSize;
		}

		final byte[] value = readBytes(valSize, context);

		Headers headers = readHeaders(context);

		return new ConsumerRecord<>(context.topic, context.partition, context.offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
			(long) NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, key, value, headers);
	}

	private Headers readHeaders(final ReadContext context) throws IOException {
		boolean isTheHeaderMarkerNext = peek(context, HEADER_MARKER_SIZE)
			.filter(next -> Arrays.equals(next, new byte[]{HEADER_MARKER}))
			.isPresent();

		if (!isTheHeaderMarkerNext) {
			return new RecordHeaders();
		}

		skip(context, HEADER_MARKER_SIZE);

		int headerSize = readValueLen(context);
		byte[] headerBlock = readBytes(headerSize, context);
		return deserialiseHeaders(headerBlock);
	}

	private Headers deserialiseHeaders(final byte[] headerBlock) {
		String jsonString = new String(headerBlock);
		Type listType = new TypeToken<ArrayList<RecordHeader>>() {
		}.getType();
		List<Header> headers = GSON.fromJson(jsonString, listType);
		return new RecordHeaders(headers);
	}

	private int readValueLen(ReadContext context) throws IOException {
		final Integer len = readLen(context);
		if (len == null) {
			die(context);
		}
		return len;
	}

	private byte[] readBytes(int keySize, ReadContext context) throws IOException {
		final byte[] bytes = new byte[keySize];
		int read = 0;
		while (read < keySize) {
			final int readNow = context.data.read(bytes, read, keySize - read);
			if (readNow == -1) {
				die(context);
			}
			read += readNow;
		}
		return bytes;
	}

	private Integer readLen(ReadContext context) throws IOException {
		lenBuffer.rewind();
		int read = context.data.read(lenBuffer.array(), 0, 4);
		if (read == -1) {
			return null;
		} else if (read != 4) {
			die(context);
		}
		return lenBuffer.getInt();
	}

	private Optional<byte[]> peek(ReadContext context, int bytes) throws IOException {
		context.data.mark(bytes);
		ByteBuffer peekBuffer = ByteBuffer.allocate(bytes);
		int read = context.data.read(peekBuffer.array(), 0, bytes);
		context.data.reset();
		return read == -1 ? Optional.empty() : Optional.of(peekBuffer.array());
	}

	private long skip(ReadContext context, int bytes) throws IOException {
		return context.data.skip(bytes);
	}

	protected ConsumerRecord<byte[], byte[]> die(ReadContext context) {
		throw new DataException(String.format("Corrupt record at %s-%d:%d", context.topic, context.partition, context.offset));
	}

}
