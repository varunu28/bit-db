package com.varun.db.storage;

import com.varun.db.util.FileSystemUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KeyValueStore {

    private static final String DB_DIRECTORY = "bit-db";

    private final Map<String, String> cache;
    private final Map<String, ValueMetadata> keyToValueMetadata;

    public KeyValueStore() {
        this.cache = new HashMap<>();
        this.keyToValueMetadata = new HashMap<>();
    }

    public void set(String key, String value) {
        this.cache.put(key, value);
    }

    public String get(String key) throws Exception {
        if (!this.cache.containsKey(key)) {
            throw new Exception("Key not found");
        }
        return this.cache.get(key);
    }

    public void delete(String key) throws Exception {
        if (!this.cache.containsKey(key)) {
            throw new Exception("Key not found");
        }
        this.cache.remove(key);
    }

    public void rebuild() throws IOException {
        FileSystemUtil.createFileIfNotExists(DB_DIRECTORY, true);
        File[] files = getFilesSortedByCreationTime();
        for (File file : files) {
            processFile(file);
        }
    }

    private static File[] getFilesSortedByCreationTime() {
        File directory = new File(DB_DIRECTORY);
        File[] files = directory.listFiles();
        Arrays.sort(Objects.requireNonNull(files), (o1, o2) -> {
            long l1 = Long.parseLong(o1.getName().split("_")[1]);
            long l2 = Long.parseLong(o2.getName().split("_")[1]);
            return (int) (l2 - l1);
        });
        return files;
    }

    private void processFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while (!(line = reader.readLine()).isEmpty()) {
            FileRecord fileRecord = FileRecord.buildFileRecord(line.getBytes());
            if (!keyToValueMetadata.containsKey(fileRecord.key())) {
                keyToValueMetadata.put(fileRecord.key(), buildValueMetadata(fileRecord, file.getName()));
            }
        }
    }

    private ValueMetadata buildValueMetadata(FileRecord fileRecord, String fileName) {
        return new ValueMetadata(fileName.split("_")[1], fileRecord.valueSize(), fileRecord.valuePosition(), fileRecord.timestamp());
    }

    private record ValueMetadata(String fileId, int valueSize, int valuePosition, long timestamp) {
    }
}
