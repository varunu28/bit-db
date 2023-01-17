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
import static org.junit.Assert.assertNotNull;

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
        assertNotNull(retrievedValue);
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
        assertNotNull(retrievedValue);
        assertEquals(value, retrievedValue);

        keyValueStore.delete(key);

        thrown.expect(KeyNotFoundException.class);
        thrown.expectMessage(startsWith(String.format("Key %s not present in the storage", key)));
        keyValueStore.get(key);
    }
}