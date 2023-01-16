package com.varun.db.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.*;

public class FileSystemUtilTest {

    private static final String TEST_DIR = "test_dir";

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Before
    public void setUp() {
        new File(TEST_DIR).mkdir();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void tearDown() {
        Arrays.stream(Objects.requireNonNull(new File(TEST_DIR).listFiles()))
                .forEach(File::delete);
    }

    @Test
    public void readNBytesFromFilePointer_success() throws IOException {
        // Write a string to test file
        String fileName = TEST_DIR + "/" + System.currentTimeMillis();
        String inputString = "Hello World";

        FileWriter fileWriter = new FileWriter(fileName, true);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(inputString);
        printWriter.close();

        // inputString is of 11 bytes. We read starting from filePointer 6 which is at char 'W' and read next 5 bytes
        // i.e. we read the bytes mapping to String "World"
        byte[] data = FileSystemUtil.readNBytesFromFilePointer(fileName, 6, 5);
        String dataString = new String(data, StandardCharsets.UTF_8);

        assertEquals("World", dataString);
    }

    @Test
    public void createFileIfNotExists_successForFile() throws IOException {
        String fileName = TEST_DIR + "/" + System.currentTimeMillis();
        File file = new File(fileName);
        assertFalse(file.exists());

        FileSystemUtil.createFileIfNotExists(fileName, false);

        assertTrue(file.exists());
        assertFalse(file.isDirectory());
    }

    @Test
    public void createFileIfNotExists_successForDirectory() throws IOException {
        String fileName = TEST_DIR + "/" + System.currentTimeMillis();
        File file = new File(fileName);
        assertFalse(file.exists());

        FileSystemUtil.createFileIfNotExists(fileName, true);

        assertTrue(file.exists());
        assertTrue(file.isDirectory());
    }
}