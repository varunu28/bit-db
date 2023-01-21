package com.varun.db;

import com.varun.db.command.Command;
import com.varun.db.command.CommandFactory;
import com.varun.db.exception.InvalidCommandException;
import com.varun.db.storage.KeyValueStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.varun.db.util.FileRecordConfig.DB_DIRECTORY;

@SuppressWarnings("InfiniteLoopStatement")
public class DbServer {

    private final int port;
    private final BufferedReader reader;
    private final KeyValueStore keyValueStore;

    public DbServer(int port) throws IOException {
        this.port = port;
        this.reader = new BufferedReader(new InputStreamReader(System.in));
        this.keyValueStore = new KeyValueStore(DB_DIRECTORY);
    }

    public void start() throws IOException {
        // Background compaction process
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                keyValueStore.performCompaction();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, 5, 5, TimeUnit.SECONDS);
        try (ServerSocket ignored = new ServerSocket(port)) {
            while (true) {
                String input = reader.readLine();
                Command command = CommandFactory.parseCommand(input);
                command.execute(keyValueStore);
            }
        } catch (InvalidCommandException e) {
            throw new RuntimeException(e);
        }
    }
}
