package com.varun.db;

import com.varun.db.command.Command;
import com.varun.db.command.CommandFactory;
import com.varun.db.exception.InvalidCommandException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;

@SuppressWarnings("InfiniteLoopStatement")
public class DbServer {

    private final int port;
    private BufferedReader reader;
    private final KeyValueStore keyValueStore;

    public DbServer(int port) {
        this.port = port;
        this.reader = new BufferedReader(new InputStreamReader(System.in));
        this.keyValueStore = new KeyValueStore();
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            while (true) {
                String input = reader.readLine();
                Command command = CommandFactory.parseCommand(input);
                command.execute();
            }
        } catch (InvalidCommandException e) {
            throw new RuntimeException(e);
        }
    }
}
