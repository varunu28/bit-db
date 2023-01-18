package com.varun.db.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.varun.db.util.FileRecordConfig.*;

public record FileRecord(long timestamp, int keySize, int valueSize, String key, String value) {

    public static FileRecord buildFileRecord(byte[] bytes) {
        long timestamp = parseTimestamp(bytes);
        int keySize = parseKeySize(bytes);
        int valueSize = parseValueSize(bytes);
        String key = parseKey(bytes, keySize);
        String value = parseValue(bytes,/* valuePosition= */ KEY_OFFSET + keySize, valueSize);

        return new FileRecord(timestamp, keySize, valueSize, key, value);
    }

    private static long parseTimestamp(byte[] bytes) {
        byte[] longBytes = Arrays.copyOfRange(bytes, TIMESTAMP_OFFSET, TIMESTAMP_OFFSET + TIMESTAMP_LENGTH);
        return Longs.fromByteArray(longBytes);
    }

    private static int parseKeySize(byte[] bytes) {
        byte[] intBytes = Arrays.copyOfRange(bytes, KEY_SIZE_OFFSET, KEY_SIZE_OFFSET + KEY_SIZE_LENGTH);
        return Ints.fromByteArray(intBytes);
    }

    private static int parseValueSize(byte[] bytes) {
        byte[] intBytes = Arrays.copyOfRange(bytes, VALUE_SIZE_OFFSET, VALUE_SIZE_OFFSET + VALUE_SIZE_LENGTH);
        return Ints.fromByteArray(intBytes);
    }

    private static String parseKey(byte[] bytes, int keySize) {
        byte[] keyBytes = Arrays.copyOfRange(bytes, KEY_OFFSET, KEY_OFFSET + keySize);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    private static String parseValue(byte[] bytes, int valuePosition, int valueSize) {
        byte[] valueBytes = Arrays.copyOfRange(bytes, valuePosition, valuePosition + valueSize);
        return new String(valueBytes, StandardCharsets.UTF_8);
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Longs.toByteArray(timestamp));
        outputStream.write(Ints.toByteArray(keySize));
        outputStream.write(Ints.toByteArray(valueSize));
        outputStream.write(key.getBytes());
        outputStream.write(value.getBytes());
        return outputStream.toByteArray();
    }
}