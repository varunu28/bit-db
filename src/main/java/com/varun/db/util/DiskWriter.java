package com.varun.db.util;

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

    public String persistToDisk(byte[] bytes) throws IOException {
        checkFileMemory();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file, true)) {
            fileOutputStream.write(bytes);
            fileOutputStream.flush();
        }
        return file.getPath();
    }

    private void checkFileMemory() {
        if (file.length() >= FILE_MEMORY_THRESHOLD) {
            createNewFile(dbDirectory);
        }
    }

    private void createNewFile(String dbDirectory) {
        this.file = new File(dbDirectory + "/" + FILE_PREFIX + System.currentTimeMillis());
    }
}
