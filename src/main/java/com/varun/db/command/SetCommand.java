package com.varun.db.command;

import com.varun.db.storage.KeyValueStore;

import java.io.IOException;

public record SetCommand(String key, String value) implements Command {
    @Override
    public void execute(KeyValueStore keyValueStore) {
        try {
            keyValueStore.set(key, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
