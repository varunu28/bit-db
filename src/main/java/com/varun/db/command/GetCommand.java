package com.varun.db.command;

import com.varun.db.exception.KeyNotFoundException;
import com.varun.db.storage.KeyValueStore;

import java.io.IOException;

public record GetCommand(String key) implements Command {
    @Override
    public void execute(KeyValueStore keyValueStore) {
        try {
            System.out.println(keyValueStore.get(key));
        } catch (KeyNotFoundException e) {
            System.out.printf("Key %s not found \n", key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
