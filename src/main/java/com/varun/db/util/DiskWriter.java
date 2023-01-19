package com.varun.db.util;

import com.google.common.primitives.Ints;
import com.varun.db.storage.FileRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.varun.db.util.FileRecordConfig.FILE_MEMORY_THRESHOLD;
import static com.varun.db.util.FileRecordConfig.FILE_PREFIX;

public class DiskWriter {

    private final String dbDirectory;
    private File file;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public DiskWriter(String dbDirectory) throws IOException {
        this.dbDirectory = dbDirectory;
        createNewFile(dbDirectory);
        this.file.createNewFile();
    }

    public static DiskWriterResponse persistToDiskHelper(FileRecord fileRecord, File file) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] fileRecordBytes = fileRecord.toBytes();
        outputStream.write(Ints.toByteArray(fileRecordBytes.length));
        outputStream.write(fileRecordBytes);
        int valuePosition = (int) (
                file.length() +
                        /* recordSizeAsInteger */ 4 +
                        /* timestamp */ 8 +
                        /* key size */ 4 +
                        /* value size */ 4 +
                        /* key */ fileRecord.key().getBytes().length
        );
        try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
            fileOutputStream.write(outputStream.toByteArray());
        }
        return new DiskWriterResponse(file.getPath(), valuePosition);
    }

    private void checkFileMemory() {
        if (file.length() >= FILE_MEMORY_THRESHOLD) {
            createNewFile(dbDirectory);
        }
    }

    private void createNewFile(String dbDirectory) {
        this.file = new File(dbDirectory + "/" + FILE_PREFIX + System.currentTimeMillis());
    }

    public DiskWriterResponse persistToDisk(FileRecord fileRecord) throws IOException {
        checkFileMemory();
        return persistToDiskHelper(fileRecord, file);
    }
}
