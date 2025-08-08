import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class JdbcPlayground {
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/emp?useSSL=false";
    private static final String USER = "root";
    private static final String PASS = "";

    public static void main(String[] args) {
        try {
            loadDriver();
            try (Connection conn = openConnection()) {
                try (Statement tmp = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {}
                setupSchema(conn);
                insertSeedData(conn);
                basicSelectWithStatement(conn);
                updateWithStatement(conn);
                parameterizedSelectWithPreparedStatement(conn);
                generatedKeysInsert(conn);
                scrollableResultSetDemo(conn);
                updatableResultSetUpdateDeleteInsert(conn);
                transactionWithSavepointDemo(conn);
                batchUpdateDemo(conn);
                callableStatementDemo(conn);
                databaseMetaDataDemo(conn);
                System.out.println("\n--- DONE ---");
            }
        } catch (SQLException e) {
            printSqlException(e);
        } catch (ClassNotFoundException e) {
            System.err.println("JDBC Driver not found: " + e.getMessage());
        }
    }

    private static void loadDriver() throws ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
        System.out.println("Loaded driver: " + JDBC_DRIVER);
    }

    private static Connection openConnection() throws SQLException {
        Connection c1 = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println("Connected: " + c1);
        return c1;
    }

    private static void setupSchema(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS Employees (" +
                    "id INT PRIMARY KEY AUTO_INCREMENT," +
                    "first VARCHAR(50)," +
                    "last  VARCHAR(50)," +
                    "age   INT)");
        }
        System.out.println("Schema ready.");
    }

    private static void insertSeedData(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("DELETE FROM Employees");
            st.executeUpdate("INSERT INTO Employees(first,last,age) VALUES " +
                    "('AAA','aaa',21),('BBB','bbb',32),('CCC','ccc',43),('DDD','ddd',27),('EEE','eee',30)");
        }
        System.out.println("Seed data inserted.");
    }

    private static void basicSelectWithStatement(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = st.executeQuery("SELECT * FROM Employees")) {
            System.out.println("\n-- All Employees (Statement) --");
            while (rs.next()) {
                System.out.printf("%d\t%s\t%s\t%d%n",
                        rs.getInt("id"), rs.getString("first"), rs.getString("last"), rs.getInt("age"));
            }
        }
    }

    private static void updateWithStatement(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            int rows = st.executeUpdate("UPDATE Employees SET age=age+1 WHERE first='BBB'");
            System.out.println("\nRows updated via Statement: " + rows);
        }
    }

    private static void parameterizedSelectWithPreparedStatement(Connection conn) throws SQLException {
        String sql = "SELECT id, first, last, age FROM Employees WHERE first=? AND age>=?";
        try (PreparedStatement ps = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ps.setString(1, "EEE");
            ps.setInt(2, 30);
            try (ResultSet rs = ps.executeQuery()) {
                System.out.println("\n-- PreparedStatement filter (first='EEE' AND age>=30) --");
                if (!rs.first()) {
                    System.out.println("No records found.");
                } else {
                    rs.beforeFirst();
                    while (rs.next()) {
                        System.out.printf("%d\t%s\t%s\t%d%n",
                                rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4));
                    }
                }
            }
        }
    }

    private static void generatedKeysInsert(Connection conn) throws SQLException {
        String sql = "INSERT INTO Employees(first,last,age) VALUES (?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "NEW");
            ps.setString(2, "new");
            ps.setInt(3, 22);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    System.out.println("\nGenerated key for NEW: " + keys.getLong(1));
                }
            }
        }
    }

    private static void scrollableResultSetDemo(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = st.executeQuery("SELECT id, first FROM Employees ORDER BY id")) {
            System.out.println("\n-- Scrollable ResultSet demo --");
            if (!rs.next()) {
                System.out.println("Empty.");
                return;
            }
            rs.first();
            System.out.println("First: " + rs.getInt(1) + " " + rs.getString(2));
            rs.last();
            System.out.println("Last: " + rs.getInt(1) + " " + rs.getString(2));
            rs.previous();
            System.out.println("Prev of last: " + rs.getInt(1) + " " + rs.getString(2));
            rs.absolute(3);
            System.out.println("Absolute(3): " + rs.getInt(1) + " " + rs.getString(2));
            rs.relative(-2);
            System.out.println("Relative(-2): " + rs.getInt(1) + " " + rs.getString(2));
        }
    }

    private static void updatableResultSetUpdateDeleteInsert(Connection conn) throws SQLException {
        System.out.println("\n-- Updatable ResultSet demo --");
        try (Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet rs = st.executeQuery("SELECT id, first, last, age FROM Employees ORDER BY id")) {
            if (!rs.next()) return;
            rs.updateString("last", rs.getString("last").toUpperCase());
            rs.updateRow();
            if (rs.absolute(5)) {
                System.out.println("Deleting row id=" + rs.getInt("id"));
                rs.deleteRow();
            }
            rs.moveToInsertRow();
            rs.updateString("first", "INS");
            rs.updateString("last", "ins");
            rs.updateInt("age", 35);
            rs.insertRow();
            rs.moveToCurrentRow();
        }
    }

    private static void transactionWithSavepointDemo(Connection conn) throws SQLException {
        System.out.println("\n-- Transaction with savepoint --");
        conn.setAutoCommit(false);
        Savepoint sp1 = null;
        try (Statement st = conn.createStatement()) {
            st.executeUpdate("UPDATE Employees SET age=age+10 WHERE first='AAA'");
            sp1 = conn.setSavepoint("sp1");
            st.executeUpdate("UPDATE Employees SET age=age+10 WHERE first='EEE'");
            if (true) throw new SQLException("Simulated failure after EEE update.");
        } catch (SQLException ex) {
            System.out.println("Error occurred, rolling back to sp1: " + ex.getMessage());
            conn.rollback(sp1);
        } finally {
            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    private static void batchUpdateDemo(Connection conn) throws SQLException {
        System.out.println("\n-- Batch demo --");
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.addBatch("UPDATE Employees SET age=age+1 WHERE first='AAA'");
            st.addBatch("UPDATE EmployeesX SET age=0 WHERE first='ZZZ'");
            int[] counts = st.executeBatch();
            System.out.println("Batch counts: " + Arrays.toString(counts));
            conn.commit();
        } catch (BatchUpdateException bue) {
            System.out.println("BatchUpdateException: " + bue.getMessage());
            System.out.println("Counts so far: " + Arrays.toString(bue.getUpdateCounts()));
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private static void callableStatementDemo(Connection conn) throws SQLException {
        System.out.println("\n-- CallableStatement demo --");
        String call = "{CALL display(?,?)}";
        try (CallableStatement cs = conn.prepareCall(call, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            cs.setString(1, "AAA");
            cs.registerOutParameter(2, Types.INTEGER);
            boolean hasResult = cs.execute();
            if (hasResult) {
                try (ResultSet rs = cs.getResultSet()) {
                    while (rs.next()) {
                        System.out.printf("SP row -> %d %s %s %d%n",
                                rs.getInt(1), rs.getString(2), rs.getString(3), rs.getInt(4));
                    }
                }
            }
            System.out.println("OUT param p = " + cs.getInt(2));
        } catch (SQLException e) {
            System.out.println("Stored procedure not found or error calling it: " + e.getMessage());
        }
    }

    private static void databaseMetaDataDemo(Connection conn) throws SQLException {
        System.out.println("\n-- Metadata demo --");
        DatabaseMetaData meta = conn.getMetaData();
        System.out.println("DB Product: " + meta.getDatabaseProductName());
        System.out.println("User: " + meta.getUserName());
        System.out.println("URL: " + meta.getURL());
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM Employees LIMIT 1")) {
            ResultSetMetaData rsm = rs.getMetaData();
            System.out.println("Column count: " + rsm.getColumnCount());
            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                System.out.printf("  #%d %s (%s)%n", i, rsm.getColumnName(i), rsm.getColumnTypeName(i));
            }
        }
        boolean fwd = meta.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY);
        boolean ins = meta.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        boolean sen = meta.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE);
        System.out.printf("Supports: forwardOnly=%s, scrollInsensitive=%s, scrollSensitive=%s%n", fwd, ins, sen);
    }

    private static void printSqlException(SQLException e) {
        System.err.println("\n--- SQLException chain ---");
        SQLException cur = e;
        while (cur != null) {
            System.err.printf("SQLState=%s ErrorCode=%d Message=%s%n",
                    cur.getSQLState(), cur.getErrorCode(), cur.getMessage());
            cur = cur.getNextException();
        }
    }
}
