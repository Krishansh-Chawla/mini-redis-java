made-my-own-redis-in-java
A lightweight, multi-threaded Redis server implemented from scratch in Java, featuring raw socket networking, RESP protocol parsing, concurrent data storage, and key expiration support.

Features
  1. RESP Protocol Support: Parses arrays and command structures from raw client inputs.
  2. Multi-Threaded Architecture: Utilizes a cached thread pool (ExecutorService) to handle multiple concurrent client connections safely.
  3. Thread-Safe Data Store: Backed by Java's ConcurrentHashMap for high-performance concurrent reads and writes.
  4. Key Expiration Support: Implements optional Time-To-Live (TTL) handling via EX and PX parameters with lazy expiration cleanup.
  5. Core Commands Supported:
       PING / PONG
       ECHO
       SET (with EX / PX expiration modifiers)
       GET
       EXISTS
       DEL
       CONFIG GET

Getting Started
  Prerequisites Java Development Kit (JDK) 8 or higher.

  Running the Server
    Compile the code:javac Main.java
    Start the server (default port 6379):java Main
    (Optional) You can specify a custom port using the --port flag:java Main --port 6380
  Connecting via Redis CLI
    You can connect to your custom server using the official Redis CLI:redis-cli -p 6379
  
    Test it out with basic commands:
    127.0.0.1:6379> PINGPONG
    127.0.0.1:6379> SET mykey "Hello, Redis!" EX 10
    OK
    127.0.0.1:6379> GET mykey
    "Hello, Redis!"

Project Structure
    Main.java — Contains the raw socket server logic, RESP parser, concurrency pool, and command execution engine. 
