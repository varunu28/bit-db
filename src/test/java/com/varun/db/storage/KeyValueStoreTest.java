package com.varun.db.storage;

import com.varun.db.exception.KeyNotFoundException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;

public class KeyValueStoreTest {
    private static final String TEST_DIR = "test-dir";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void tearDown() {
        Arrays.stream(Objects.requireNonNull(new File(TEST_DIR).listFiles()))
                .forEach(File::delete);
        new File(TEST_DIR).delete();
    }

    @Test
    public void keyValueStore_setAndGetSuccess() throws IOException, KeyNotFoundException {
        KeyValueStore keyValueStore = new KeyValueStore("test-dir");

        String key = "A";
        String value = "1";
        keyValueStore.set(key, value);

        String retrievedValue = keyValueStore.get(key);
        assertEquals(value, retrievedValue);
    }

    @Test
    public void keyValueStore_keyNotFound() throws IOException, KeyNotFoundException {
        KeyValueStore keyValueStore = new KeyValueStore(TEST_DIR);
        String key = "A";

        thrown.expect(KeyNotFoundException.class);
        thrown.expectMessage(startsWith(String.format("Key %s not present in the storage", key)));
        keyValueStore.get(key);
    }

    @Test
    public void keyValueStore_deleteSuccess() throws IOException, KeyNotFoundException {
        KeyValueStore keyValueStore = new KeyValueStore(TEST_DIR);
        String key = "A";
        String value = "1";
        keyValueStore.set(key, value);

        String retrievedValue = keyValueStore.get(key);
        assertEquals(value, retrievedValue);

        keyValueStore.delete(key);

        thrown.expect(KeyNotFoundException.class);
        thrown.expectMessage(startsWith(String.format("Key %s not present in the storage", key)));
        keyValueStore.get(key);
    }

    @Test
    public void keyValueStore_rebuildSuccess() throws IOException, KeyNotFoundException {
        KeyValueStore keyValueStore = new KeyValueStore(TEST_DIR);
        String key = "A";
        String value = "1";
        keyValueStore.set(key, value);

        String retrievedValue = keyValueStore.get(key);
        assertEquals(value, retrievedValue);

        // Remove the previous KeyValueStore instance and create a new instance which will trigger the rebuild
        keyValueStore = new KeyValueStore(TEST_DIR);

        retrievedValue = keyValueStore.get(key);
        assertEquals(value, retrievedValue);

        // As the key-value store is instantiated twice, 2 DB files should have been created. This is because a file is
        // considered immutable once the DB connection is closed and a new file is created when the connection is
        // re-established
        int numberOfFiles = Objects.requireNonNull(new File(TEST_DIR).listFiles()).length;
        assertEquals(2, numberOfFiles);
    }

    @Test
    public void keyValueStore_compactionSuccess() throws IOException, KeyNotFoundException, InterruptedException {
        KeyValueStore keyValueStore = new KeyValueStore(TEST_DIR);
        keyValueStore.set("A", "1");
        sleep();
        keyValueStore = new KeyValueStore(TEST_DIR);
        keyValueStore.set("A", "2");
        sleep();
        keyValueStore = new KeyValueStore(TEST_DIR);
        keyValueStore.set("A", "3");
        sleep();
        keyValueStore = new KeyValueStore(TEST_DIR);

        keyValueStore.performCompaction();

        assertEquals(2, Objects.requireNonNull(new File(TEST_DIR).listFiles()).length);

        String retrievedValue = keyValueStore.get("A");
        assertEquals("3", retrievedValue);
    }

    private void sleep() throws InterruptedException {
        Thread.sleep(100L);
    }
}