package com.varun.db;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        DbServer server = new DbServer(8000);
        server.start();
        System.out.printf("BitDb server started on port: %d\n", 8000);
    }
}
