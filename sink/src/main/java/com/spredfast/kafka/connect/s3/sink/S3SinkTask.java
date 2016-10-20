package com.spredfast.kafka.connect.s3.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.spredfast.kafka.connect.s3.Constants;
import com.spredfast.kafka.connect.s3.Converters;
import com.spredfast.kafka.connect.s3.S3;
import com.spredfast.kafka.connect.s3.ToStringWithDelimiterConverter;


public class S3SinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(S3SinkTask.class);

	private Map<String, String> config;

	private Map<TopicPartition, BlockGZIPFileWriter> tmpFiles;

	private long GZIPChunkThreshold = 67108864;

	private S3Writer s3;

	private Converter keyConverter;

	private Converter valueConverter;

	public S3SinkTask() {
		tmpFiles = new HashMap<>();
	}

	@Override
	public String version() {
		return Constants.VERSION;
	}

	@Override
	public void start(Map<String, String> props) throws ConnectException {
		config = props;
		String chunkThreshold = config.get("compressed_block_size");
		if (chunkThreshold != null) {
			try {
				this.GZIPChunkThreshold = Long.parseLong(chunkThreshold);
			} catch (NumberFormatException nfe) {
				// keep default
			}
		}

		keyConverter = Converters.buildConverter(props, "key.converter", true, null);
		valueConverter = Converters.buildConverter(props, "value.converter", false, ToStringWithDelimiterConverter.class);

		String bucket = config.get("s3.bucket");
		String prefix = config.get("s3.prefix");
		if (bucket == null || Objects.equals(bucket, "")) {
			throw new ConnectException("S3 bucket must be configured");
		}
		if (prefix == null) {
			prefix = "";
		}
		AmazonS3 s3Client = S3.s3client(config);

		s3 = new S3Writer(bucket, prefix, s3Client);

		// Recover initial assignments
		Set<TopicPartition> assignment = context.assignment();
		recoverAssignment(assignment);
	}

	private static void configureConverter(Map<String, ?> props, String prefixProp, boolean isKey, Converter converter) {
		if (converter instanceof Configurable) {
			((Configurable) converter).configure(props);
		}

		// grab any properties intended for the converter
		Map<String, Object> subKeys = new LinkedHashMap<>();
		String prefix = prefixProp + ".";
		for (String p : props.keySet()) {
			if (p.startsWith(prefix)) {
				subKeys.put(p.substring(prefix.length()), props.get(p));
			}
		}

		converter.configure(subKeys, isKey);
	}

	private String firstPresent(Map<String, String> props, String... keys) {
		for (String key : keys) {
			if (props.containsKey(key)) {
				return props.get(key);
			}
		}
		return null;
	}

	@Override
	public void stop() throws ConnectException {
		// We could try to be smart and flush buffer files to be resumed
		// but for now we just start again from where we got to in S3 and overwrite any
		// buffers on disk.
	}

	@Override
	public void put(Collection<SinkRecord> records) throws ConnectException {
		for (SinkRecord record : records) {
			try {
				String topic = record.topic();
				int partition = record.kafkaPartition();
				TopicPartition tp = new TopicPartition(topic, partition);
				BlockGZIPFileWriter buffer = tmpFiles.get(tp);
				if (buffer == null) {
					log.error("Trying to put {} records to partition {} which doesn't exist yet", records.size(), tp);
					throw new ConnectException("Trying to put records for a topic partition that has not be assigned");
				}
				buffer.write(record);
			} catch (IOException e) {
				throw new RetriableException("Failed to write to buffer", e);
			}
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
		// Don't rely on offsets passed. They have some quirks like including topic partitions that just
		// got revoked (i.e. we have deleted the writer already). Not sure if this is intended...
		// https://twitter.com/mr_paul_banks/status/702493772983177218

		// Instead iterate over the writers we do have and get the offsets directly from them.
		for (Map.Entry<TopicPartition, BlockGZIPFileWriter> entry : tmpFiles.entrySet()) {
			TopicPartition tp = entry.getKey();
			BlockGZIPFileWriter writer = entry.getValue();
			if (writer.getNumRecords() == 0) {
				// Not done anything yet
				log.info("No new records for partition {}", tp);
				continue;
			}
			try {
				writer.close();

				long nextOffset = s3.putChunk(writer.getDataFilePath(), writer.getIndexFilePath(), tp);

				// Now reset writer to a new one
				tmpFiles.put(tp, this.createNextBlockWriter(tp, nextOffset));
				log.info("Successfully uploaded chunk for {} now at offset {}", tp, nextOffset);
			} catch (FileNotFoundException fnf) {
				throw new ConnectException("Failed to find local dir for temp files", fnf);
			} catch (IOException e) {
				throw new RetriableException("Failed S3 upload", e);
			}
		}
	}

	private BlockGZIPFileWriter createNextBlockWriter(TopicPartition tp, long nextOffset) throws ConnectException, IOException {
		String name = String.format("%s-%05d", tp.topic(), tp.partition());
		String path = config.get("local.buffer.dir");
		if (path == null) {
			throw new ConnectException("No local buffer file path configured");
		}
		return new BlockGZIPFileWriter(name, path, nextOffset, this.GZIPChunkThreshold, keyConverter, valueConverter);
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) throws ConnectException {
		recoverAssignment(partitions);
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) throws ConnectException {
		for (TopicPartition tp : partitions) {
			// See if this is a new assignment
			BlockGZIPFileWriter writer = this.tmpFiles.remove(tp);
			if (writer != null) {
				log.info("Revoked partition {} deleting buffer", tp);
				try {
					writer.close();
					writer.delete();
				} catch (IOException ioe) {
					throw new ConnectException("Failed to resume TopicPartition form S3", ioe);
				}
			}
		}
	}

	private void recoverAssignment(Collection<TopicPartition> partitions) throws ConnectException {
		for (TopicPartition tp : partitions) {
			// See if this is a new assignment
			if (this.tmpFiles.get(tp) == null) {
				log.info("Assigned new partition {} creating buffer writer", tp);
				try {
					recoverPartition(tp);
				} catch (IOException ioe) {
					throw new ConnectException("Failed to resume TopicPartition from S3", ioe);
				}
			}
		}
	}

	// HACK - added to SinkTask in 0.10
	public void open(Collection<TopicPartition> partitions) {
		this.onPartitionsAssigned(partitions);
	}

	// HACK - added to SinkTask in 0.10
	public void close(Collection<TopicPartition> partitions) {
		this.onPartitionsRevoked(partitions);
	}

	private void recoverPartition(TopicPartition tp) throws IOException {
		this.context.pause(tp);

		// Recover last committed offset from S3
		long offset = s3.fetchOffset(tp);

		log.info("Recovering partition {} from offset {}", tp, offset);

		BlockGZIPFileWriter w = createNextBlockWriter(tp, offset);
		tmpFiles.put(tp, w);

		this.context.offset(tp, offset);
		this.context.resume(tp);
	}
}