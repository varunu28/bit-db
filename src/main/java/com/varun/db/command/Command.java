package com.varun.db.command;

import com.varun.db.storage.KeyValueStore;

public interface Command {

    void execute(KeyValueStore keyValueStore);
}
