package com.spredfast.kafka.connect.s3;

public class Constants {
	public static final String VERSION = "0.0.1";

	public static final int LENGTH_FIELD_SIZE = 4;

	public static final byte[] NO_BYTES = {};

	public static final byte HEADER_MARKER = (byte) 0xF6;

	public static final int HEADER_MARKER_SIZE = 1;

	private Constants() {
	}
}
