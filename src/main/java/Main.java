import com.varun.db.DbServer;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        DbServer server = new DbServer(8000);
        server.start();
    }
}
