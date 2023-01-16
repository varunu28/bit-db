package com.varun.db.command;

import com.varun.db.storage.KeyValueStore;

public record DeleteCommand(String key) implements Command {
    @Override
    public void execute(KeyValueStore keyValueStore) {

    }
}
