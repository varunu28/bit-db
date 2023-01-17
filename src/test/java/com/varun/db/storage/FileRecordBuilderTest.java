package com.varun.db.storage;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class FileRecordBuilderTest {

    @Test
    public void buildFileRecord_successForSingleByteKeyValue() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        long timestamp = System.currentTimeMillis();
        int keySize = 1;
        int valueSize = 1;
        String key = "A";
        String value = "B";
        outputStream.write(Longs.toByteArray(timestamp));
        outputStream.write(Ints.toByteArray(keySize));
        outputStream.write(Ints.toByteArray(valueSize));
        outputStream.write(key.getBytes());
        outputStream.write(value.getBytes());

        FileRecord fileRecord = FileRecord.buildFileRecord(outputStream.toByteArray());

        assertEquals(timestamp, fileRecord.timestamp());
        assertEquals(keySize, fileRecord.keySize());
        assertEquals(valueSize, fileRecord.valueSize());
        assertEquals(key, fileRecord.key());
        assertEquals(value, fileRecord.value());
        /*
         * +----------------------------------+---------+-------------------------------+
         * | Timestamp(8 bytes) | Key Size(4 bytes) | Value Size(4 bytes) | Key | Value |
         * +----------------------------------+---------+-------------------------------+
         * |         0 - 7      |        8 - 11     |      12 - 15        |  16 |   17  |
         * +----------------------------------+---------+------------------------+------+
         * */
        assertEquals(17, fileRecord.getValuePosition());
    }

    @Test
    public void buildFileRecord_successForMultiByteKeyValue() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        long timestamp = System.currentTimeMillis();
        int keySize = 3;
        int valueSize = 4;
        String key = "ABC";
        String value = "PQRS";
        outputStream.write(Longs.toByteArray(timestamp));
        outputStream.write(Ints.toByteArray(keySize));
        outputStream.write(Ints.toByteArray(valueSize));
        outputStream.write(key.getBytes());
        outputStream.write(value.getBytes());

        FileRecord fileRecord = FileRecord.buildFileRecord(outputStream.toByteArray());

        assertEquals(timestamp, fileRecord.timestamp());
        assertEquals(keySize, fileRecord.keySize());
        assertEquals(valueSize, fileRecord.valueSize());
        assertEquals(key, fileRecord.key());
        assertEquals(value, fileRecord.value());
        /*
         * +----------------------------------+---------+-----------------------------------------+
         * | Timestamp(8 bytes) | Key Size(4 bytes) | Value Size(4 bytes) |     Key  |     Value  |
         * +----------------------------------+---------+-----------------------------------------+
         * |         0 - 7      |        8 - 11     |      12 - 15        |  16 - 18 |   19 - 22  |
         * +----------------------------------+---------+------------------------+----------------+
         * */
        assertEquals(19, fileRecord.getValuePosition());
    }
}