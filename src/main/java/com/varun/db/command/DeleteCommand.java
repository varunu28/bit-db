package com.varun.db.command;

import com.varun.db.exception.KeyNotFoundException;
import com.varun.db.storage.KeyValueStore;

import java.io.IOException;

public record DeleteCommand(String key) implements Command {
    @Override
    public void execute(KeyValueStore keyValueStore) {
        try {
            keyValueStore.delete(key);
        } catch (KeyNotFoundException e) {
            System.out.printf("Key %s not found \n", key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
