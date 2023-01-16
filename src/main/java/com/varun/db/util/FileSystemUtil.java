package com.varun.db.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileSystemUtil {

    public static byte[] readNBytesFromFilePointer(String fileName, long filePointer, int bytes) throws IOException {
        byte[] data = new byte[bytes];
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(new File(fileName), "r")) {
            randomAccessFile.seek(filePointer);
            randomAccessFile.read(data, 0, bytes);
        }
        return data;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createFileIfNotExists(String fileName, boolean isDirectory) throws IOException {
        File file = new File(fileName);
        if (isDirectory) {
            file.mkdir();
        } else {
            file.createNewFile();
        }
    }
}
