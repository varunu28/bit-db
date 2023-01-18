package com.varun.db.storage;

import com.google.common.primitives.Ints;
import com.varun.db.exception.KeyNotFoundException;
import com.varun.db.util.DiskWriter;
import com.varun.db.util.DiskWriterResponse;
import com.varun.db.util.FileSystemUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KeyValueStore {

    private static final String TOMBSTONE_VALUE = "tombstone";

    private final Map<String, String> cache;
    private final Map<String, ValueMetadata> keyToValueMetadata;
    private final DiskWriter diskWriter;
    private final String dbDirectory;

    public KeyValueStore(String dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        this.cache = new HashMap<>();
        this.keyToValueMetadata = new HashMap<>();
        FileSystemUtil.createFileIfNotExists(dbDirectory, true);
        rebuild();
        this.diskWriter = new DiskWriter(this.dbDirectory);
    }

    private static File[] getFilesSortedByCreationTime(String dbDirectory) {
        File directory = new File(dbDirectory);
        File[] files = directory.listFiles();
        Arrays.sort(Objects.requireNonNull(files), (o1, o2) -> {
            long l1 = Long.parseLong(o1.getName().split("_")[1]);
            long l2 = Long.parseLong(o2.getName().split("_")[1]);
            return (int) (l2 - l1);
        });
        return files;
    }

    public String get(String key) throws KeyNotFoundException, IOException {
        if (!this.keyToValueMetadata.containsKey(key)) {
            throw new KeyNotFoundException(String.format("Key %s not present in the storage", key));
        }
        if (this.cache.containsKey(key)) {
            System.out.println("Cache hit");
            return this.cache.get(key);
        }
        ValueMetadata valueMetadata = this.keyToValueMetadata.get(key);
        byte[] bytes = FileSystemUtil.readNBytesFromFilePointer(valueMetadata.fileId, valueMetadata.valuePosition, valueMetadata.valueSize);
        String value = new String(bytes, StandardCharsets.UTF_8);
        cache.put(key, value);

        return value;
    }

    public void set(String key, String value) throws IOException {
        FileRecord fileRecord = new FileRecord(
                /* timestamp= */ System.currentTimeMillis(),
                /* keySize= */ key.getBytes().length,
                /* valSize= */ value.getBytes().length,
                key,
                value);

        // We remove the key from cache if it is present. Cache is populated only during the get path.
        this.cache.remove(key);

        DiskWriterResponse diskWriterResponse = this.diskWriter.persistToDisk(fileRecord);
        this.keyToValueMetadata.put(key,
                buildValueMetadata(fileRecord, diskWriterResponse.fileName(), diskWriterResponse.valuePosition()));
    }

    public void delete(String key) throws KeyNotFoundException, IOException {
        if (!keyToValueMetadata.containsKey(key)) {
            throw new KeyNotFoundException(String.format("Key %s not present in the storage", key));
        }
        this.cache.remove(key);
        this.diskWriter.persistToDisk(new FileRecord(
                /* timestamp= */ System.currentTimeMillis(),
                /* keySize= */ key.getBytes().length,
                /* valSize= */ TOMBSTONE_VALUE.getBytes().length,
                key,
                TOMBSTONE_VALUE));
        this.keyToValueMetadata.remove(key);
    }

    private void rebuild() throws IOException {
        File[] files = getFilesSortedByCreationTime(dbDirectory);
        for (File file : files) {
            processFile(file);
        }
    }

    private void processFile(File file) throws IOException {
        byte[] bytes = new byte[(int) file.length()];
        try (FileInputStream inputStream = new FileInputStream(file)) {
            inputStream.read(bytes);
        }
        int byteCursor = 0;
        while (byteCursor < bytes.length) {
            int recordSize = Ints.fromByteArray(Arrays.copyOfRange(bytes, byteCursor, byteCursor + 4));
            byteCursor += 4;
            byte[] record = Arrays.copyOfRange(bytes, byteCursor, byteCursor + recordSize);
            FileRecord fileRecord = FileRecord.buildFileRecord(record);
            int valuePosition = (
                    byteCursor +
                            /* timestamp */ 8 +
                            /* key size */ 4 +
                            /* value size */ 4 +
                            /* key */ fileRecord.key().getBytes().length
            );
            if (!keyToValueMetadata.containsKey(fileRecord.key())) {
                keyToValueMetadata.put(fileRecord.key(),
                        buildValueMetadata(fileRecord, file.getPath(), valuePosition));
            }
            byteCursor += recordSize;
        }
    }

    private ValueMetadata buildValueMetadata(FileRecord fileRecord, String fileName, int valuePosition) {
        return new ValueMetadata(fileName, fileRecord.valueSize(), valuePosition, fileRecord.timestamp());
    }

    private record ValueMetadata(String fileId, int valueSize, int valuePosition, long timestamp) {
    }
}
