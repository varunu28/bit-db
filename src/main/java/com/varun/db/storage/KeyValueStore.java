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
import java.util.*;

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

    private static File[] getFilesSortedByCreationTime(String dbDirectory, boolean desc) {
        File directory = new File(dbDirectory);
        File[] files = directory.listFiles();
        Arrays.sort(Objects.requireNonNull(files), (o1, o2) -> {
            long l1 = Long.parseLong(o1.getName().split("_")[1]);
            long l2 = Long.parseLong(o2.getName().split("_")[1]);
            return desc ? (int) (l2 - l1) : (int) (l1 - l2);
        });
        return files;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void performCompaction() throws IOException {
        /*
          How does compaction works?
          1. List down files all but the currently opened file
            - Edge case if there is only one file in addition to currently opened file, then we don't perform compaction
          2. Start iterating from the most recent file to the least recent file
          3. Build a mapping of key to ValueMetadata with most recent record for each key
          4. Create a new file with name equal to name of most recent file in files that are listed in step 1 and suffix
             it with a # sign
          5. Write all the data from mapping to the file
          6. Delete all listed files
          7. Rename the file to remove the # sign
          8. Iterate through the mapping and update original keyToValueMetadata

          How is node failure handled?:
          1. DB node crashes after building the mapping but before writing to new file
          => No action needed. Compaction will restart the process.
          2. DB nodes crashes after creating new file but before deleting the listed files.
          => During rebuild, we check if there is a file with # sign and if there is a corresponding file without the
          # sign. If yes then we remove the file with # sign. This brings back us to the scenario mentioned in 1.
          3. DB nodes crashes after creating new file and after deleting the listed files.
          => During rebuild, if we see a file with # sign but no corresponding file without the # sign, then we rename
          the file to remove # sign and move ahead with rebuild. As this file contains the merged data, the
          keyToValueMetadata will contain correct info
          */
        // Validation check
        long numberOfFiles = Objects.requireNonNull(new File(dbDirectory).listFiles()).length;
        if (numberOfFiles < 3) {
            return;
        }
        // List down all but currently opened file
        List<File> filesToCompact = Arrays.stream(getFilesSortedByCreationTime(dbDirectory, false))
                .limit(numberOfFiles - 1)
                .toList();
        // Rebuild keyToValueMetadata by processing all files
        Map<String, ValueMetadata> compactedFilesKeyToValueMetadata = new HashMap<>();
        Map<String, String> keyToValueMapping = new HashMap<>();
        for (int i = filesToCompact.size() - 1; i >= 0; i--) {
            Map<String, String> tempMapping = processFileAndReturnKeyValueMapping(filesToCompact.get(i),
                    compactedFilesKeyToValueMetadata);
            for (String key : tempMapping.keySet()) {
                if (!keyToValueMapping.containsKey(key)) {
                    keyToValueMapping.put(key, tempMapping.get(key));
                }
            }
        }
        // Create new file with '#' suffix
        String compactedFileName = filesToCompact.get(filesToCompact.size() - 1).getPath() + "#";
        File compactedFile = new File(compactedFileName);
        compactedFile.createNewFile();
        // Write the mapping to new file
        for (Map.Entry<String, ValueMetadata> entry : compactedFilesKeyToValueMetadata.entrySet()) {
            FileRecord fileRecord = new FileRecord(
                    entry.getValue().timestamp,
                    entry.getKey().getBytes().length,
                    entry.getValue().valueSize,
                    entry.getKey(),
                    keyToValueMapping.get(entry.getKey())
            );
            DiskWriter.persistToDiskHelper(fileRecord, compactedFile);
        }
        // Delete the listed files
        filesToCompact.forEach(File::delete);
        // Rename the compacted file to remove '#' suffix
        File renamedFileWithoutSuffix = new File(compactedFileName.substring(0, compactedFileName.length() - 1));
        compactedFile.renameTo(renamedFileWithoutSuffix);
        // Update existing mapping to new file
        for (Map.Entry<String, ValueMetadata> entry : compactedFilesKeyToValueMetadata.entrySet()) {
            if (this.keyToValueMetadata.containsKey(entry.getKey()) &&
                    this.keyToValueMetadata.get(entry.getKey()).timestamp == entry.getValue().timestamp &&
                    !this.keyToValueMetadata.get(entry.getKey()).fileId.equals(entry.getValue().fileId)) {
                this.keyToValueMetadata.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private void rebuild() throws IOException {
        File[] files = getFilesSortedByCreationTime(dbDirectory, true);
        for (File file : files) {
            processFile(file, this.keyToValueMetadata);
        }
    }

    private void processFile(File file, Map<String, ValueMetadata> keyToValueMetadata) throws IOException {
        processFileAndReturnKeyValueMapping(file, keyToValueMetadata);
    }

    private Map<String, String> processFileAndReturnKeyValueMapping(File file, Map<String, ValueMetadata> keyToValueMetadata) throws IOException {
        Map<String, String> keyToValueMapping = new HashMap<>();
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
            if (!keyToValueMetadata.containsKey(fileRecord.key()) ||
                    keyToValueMetadata.get(fileRecord.key()).fileId.equals(file.getPath())) {
                keyToValueMetadata.put(fileRecord.key(),
                        buildValueMetadata(fileRecord, file.getPath(), valuePosition));
                keyToValueMapping.put(fileRecord.key(), fileRecord.value());
            }
            byteCursor += recordSize;
        }
        return keyToValueMapping;
    }

    private ValueMetadata buildValueMetadata(FileRecord fileRecord, String fileName, int valuePosition) {
        return new ValueMetadata(fileName, fileRecord.valueSize(), valuePosition, fileRecord.timestamp());
    }

    private record ValueMetadata(String fileId, int valueSize, int valuePosition, long timestamp) {
    }
}
