package com.varun.db.command;

import com.varun.db.storage.KeyValueStore;

public record SetCommand(String key, String value) implements Command {
    @Override
    public void execute(KeyValueStore keyValueStore) {

    }
}
