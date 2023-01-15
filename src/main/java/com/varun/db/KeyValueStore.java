package com.varun.db;

import java.util.HashMap;
import java.util.Map;

public class KeyValueStore {

    private final Map<String, String> store;

    public KeyValueStore() {
        this.store = new HashMap<>();
    }

    public void set(String key, String value) {
        this.store.put(key, value);
    }

    public String get(String key) throws Exception {
        if (!this.store.containsKey(key)) {
            throw new Exception("Key not found");
        }
        return this.store.get(key);
    }

    public void delete(String key) throws Exception {
        if (!this.store.containsKey(key)) {
            throw new Exception("Key not found");
        }
        this.store.remove(key);
    }
}
