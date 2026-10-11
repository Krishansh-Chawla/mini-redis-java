import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    // Wrapper class to store values alongside their optional expiration time
    private static class RedisObject {
        String value;
        Long expiryTime; // Epoch timestamp in milliseconds, null if no expiry

        public RedisObject(String value, Long expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }

        public boolean isExpired() {
            if (expiryTime == null) return false;
            return System.currentTimeMillis() > expiryTime;
        }
    }

    // In-memory thread-safe data store
    private static final ConcurrentHashMap<String, RedisObject> store = new ConcurrentHashMap<>();
    
    // Config store for compatibility (e.g., CONFIG GET dir / dbfilename)
    private static final ConcurrentHashMap<String, String> configStore = new ConcurrentHashMap<>();

    static {
        configStore.put("dir", "/tmp");
        configStore.put("dbfilename", "dump.rdb");
    }

    public static void main(String[] args) {
        int port = 6379;

        // Parse command line arguments for --port flag
        for (int i = 0; i < args.length; i++) {
            if ("--port".equals(args[i]) && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            }
        }

        System.out.println("Redis Server started on port " + port + "...");
        ExecutorService threadPool = Executors.newCachedThreadPool();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            System.out.println("Server IOException: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))
        ) {
            while (true) {
                String line = reader.readLine();
                if (line == null) break;

                // RESP Protocol array parsing (e.g., *1, *3)
                if (line.startsWith("*")) {
                    int numElements = Integer.parseInt(line.substring(1));
                    String[] args = new String[numElements];

                    for (int i = 0; i < numElements; i++) {
                        reader.readLine(); // skip size descriptor line (e.g., $4)
                        args[i] = reader.readLine(); // read actual command/argument
                    }

                    processCommand(args, outputStream);
                }
            }
        } catch (IOException e) {
            // Client closed connection
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {}
        }
    }

    private static void processCommand(String[] args, OutputStream outputStream) throws IOException {
        if (args.length == 0) return;
        String command = args[0].toUpperCase();

        switch (command) {
            case "PING":
                outputStream.write("+PONG\r\n".getBytes());
                break;

            case "ECHO":
                if (args.length > 1) {
                    writeBulkString(outputStream, args[1]);
                } else {
                    outputStream.write("$-1\r\n".getBytes());
                }
                break;

            case "SET":
                if (args.length >= 3) {
                    String key = args[1];
                    String value = args[2];
                    Long expiryTime = null;

                    // Parse optional arguments like EX (seconds) or PX (milliseconds)
                    if (args.length >= 5) {
                        String option = args[3].toUpperCase();
                        long duration = Long.parseLong(args[4]);
                        if ("EX".equals(option)) {
                            expiryTime = System.currentTimeMillis() + (duration * 1000);
                        } else if ("PX".equals(option)) {
                            expiryTime = System.currentTimeMillis() + duration;
                        }
                    }

                    store.put(key, new RedisObject(value, expiryTime));
                    outputStream.write("+OK\r\n".getBytes());
                }
                break;

            case "GET":
                if (args.length >= 2) {
                    String key = args[1];
                    RedisObject obj = store.get(key);

                    if (obj == null || obj.isExpired()) {
                        if (obj != null) {
                            store.remove(key); // Lazy expiration cleanup
                        }
                        outputStream.write("$-1\r\n".getBytes());
                    } else {
                        writeBulkString(outputStream, obj.value);
                    }
                }
                break;

            case "EXISTS":
                long existingCount = 0;
                for (int i = 1; i < args.length; i++) {
                    String key = args[i];
                    RedisObject obj = store.get(key);
                    if (obj != null && !obj.isExpired()) {
                        existingCount++;
                    } else if (obj != null) {
                        store.remove(key);
                    }
                }
                outputStream.write((":" + existingCount + "\r\n").getBytes());
                break;

            case "DEL":
                long deletedCount = 0;
                for (int i = 1; i < args.length; i++) {
                    String key = args[i];
                    RedisObject obj = store.get(key);
                    if (obj != null) {
                        if (!obj.isExpired()) {
                            deletedCount++;
                        }
                        store.remove(key);
                    }
                }
                outputStream.write((":" + deletedCount + "\r\n").getBytes());
                break;

            case "CONFIG":
                if (args.length >= 3 && "GET".equalsIgnoreCase(args[1])) {
                    String param = args[2];
                    String val = configStore.get(param);
                    if (val != null) {
                        // CONFIG GET returns an array of two elements: [parameter_name, parameter_value]
                        outputStream.write("*2\r\n".getBytes());
                        writeBulkString(outputStream, param);
                        writeBulkString(outputStream, val);
                    } else {
                        outputStream.write("*0\r\n".getBytes());
                    }
                } else {
                    outputStream.write("*0\r\n".getBytes());
                }
                break;

            default:
                outputStream.write(("-ERR unknown command '" + command + "'\r\n").getBytes());
                break;
        }
        outputStream.flush();
    }

    // Helper method to format RESP bulk strings properly
    private static void writeBulkString(OutputStream out, String str) throws IOException {
        out.write(("$" + str.length() + "\r\n" + str + "\r\n").getBytes());
    }
}