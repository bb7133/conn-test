import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnBench {

    private static final String HOST = "127.0.0.1";
    private static final String PORT = "4000";
    private static final String USER = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final int NUM_TABLES = 10;
    private static final int SIZE_TABLE = 100000;
    private static final int NUM_THREADS = 11000;
    private static final int NUM_IDLE = 10000;
    private static final int NUM_QUERIES = 1000;
    private static final long IDLE_INTERVAL = 5000; // in milliseconds
    private static final long QUERY_INTERVAL = 5000; // in milliseconds
    private static final long REPORT_PERIOD = 10000; // in milliseconds
	private static final long THREAD_START_DELAY = 20; // in milliseconds

    private static AtomicInteger connectionCount = new AtomicInteger(0);

    private static void pointGet(Connection connection) throws SQLException {
        Random random = new Random();
        int randomID = random.nextInt(SIZE_TABLE);
        int randomTable = random.nextInt(NUM_TABLES) + 1;
        String query = "SELECT * FROM sbtest" + randomTable + " WHERE id = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, randomID);
            ResultSet resultSet = statement.executeQuery();
            // Process the result set if needed
        }
    }

    private static void executeQueries(Connection connection) {
        connectionCount.incrementAndGet();
        try {
            while (true) {
                pointGet(connection);
                Thread.sleep(QUERY_INTERVAL);
            }
        } catch (InterruptedException | SQLException e) {
            e.printStackTrace();
        } finally {
            connectionCount.decrementAndGet();
        }
    }

	private static void idle(Connection connection) {
        connectionCount.incrementAndGet();
        try {
            while (true) {
				// Do nothing
				// ResultSet rs = stmt.executeQuery("SELECT SLEEP(1)");
                Thread.sleep(IDLE_INTERVAL);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            connectionCount.decrementAndGet();
        }
	}

    private static void reportConnections() {
        while (true) {
            try {
                Thread.sleep(REPORT_PERIOD);
                System.out.println("Number of running connections: " + connectionCount.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

		// Start thread to report connections
        executorService.submit(ConnBench::reportConnections);

        // Start idle connections
        for (int i = 0; i < NUM_IDLE; i++) {
			try {
                Thread.sleep(THREAD_START_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executorService.submit(() -> {
                try {
                    Connection connection = DriverManager.getConnection(
                            "jdbc:mysql://" + HOST + ":" + PORT + "/" + DATABASE, USER, PASSWORD);
                    idle(connection);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        // Start threads for executing queries
        for (int i = 0; i < NUM_THREADS - NUM_IDLE; i++) {
			try {
                Thread.sleep(THREAD_START_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            executorService.submit(() -> {
                try {
                    Connection connection = DriverManager.getConnection(
                            "jdbc:mysql://" + HOST + ":" + PORT + "/" + DATABASE, USER, PASSWORD);
                    executeQueries(connection);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        // Shutdown executor service
        executorService.shutdown();

        try {
            // Wait for all threads to finish
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
