/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FunctionsTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM data;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS data;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE data (" +
                    "int_val INT, " +
                    "double_val DECIMAL," +
                    "str_i_val VARCHAR(32)," + // Integer as string
                    "str_a_val VARCHAR(32)," + // Alpha string
//                     "bool_val BOOL," +
                    "is_null INT)";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);

        String sql = "INSERT INTO data (" +
                     "int_val, double_val, str_i_val, str_a_val, " +
//                      "bool_val, " + 
                     "is_null " +
                     ") VALUES (?, ?, ?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // Non-Null Values
        int idx = 1;
        pstmt.setInt(idx++, 123);
        pstmt.setDouble(idx++, 12.34);
        pstmt.setString(idx++, "123456");
        pstmt.setString(idx++, "AbCdEf");
//         pstmt.setBoolean(idx++, true);
        pstmt.setInt(idx++, 0);
//         pstmt.setBoolean(idx++, false);
        pstmt.addBatch();
        
        // Null Values
        idx = 1;
        pstmt.setNull(idx++, java.sql.Types.INTEGER);
        pstmt.setNull(idx++, java.sql.Types.DOUBLE);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
//         pstmt.setNull(idx++, java.sql.Types.BOOLEAN);
        pstmt.setInt(idx++, 1);
//         pstmt.setBoolean(idx++, true);
        pstmt.addBatch();
        
        pstmt.executeBatch();
    }

    /**
     * Setup for each test, execute before each test
     * reconnect and setup default table
     */
    @Before
    public void setup() throws SQLException {
        conn = makeDefaultConnection();
        conn.setAutoCommit(true);
        initDatabase();
    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table
     */
    @After
    public void teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * UDF statement tests
     * ---------------------------------------------
     */
     
     private void checkIntegerFunc(String func_name, String col_name, boolean is_null, Integer expected) throws SQLException {
         String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                    func_name, col_name, (is_null ? 1 : 0));
         
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql);
         boolean exists = rs.next();
         assert(exists);
         if (is_null) {
             checkIntRow(rs, new String[]{"result"}, new Integer[]{null});
         } else {
             checkIntRow(rs, new String[]{"result"}, new Integer[]{expected});
         }
         assertNoMoreRows(rs);
    }

    private void checkDoubleFunc(String func_name, String col_name, boolean is_null, Double expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));
                                   
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{null});
        } else {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{expected});
        }
        assertNoMoreRows(rs);
    }
    
    private void checkStringFunc(String func_name, String col_name, boolean is_null, String expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkStringRow(rs, new String[]{"result"}, new String[]{null});
        } else {
            checkStringRow(rs, new String[]{"result"}, new String[]{expected});
        }
        assertNoMoreRows(rs);
    }

    private void checkStringPositionFunc(String func_name, String substring, String col_name, boolean is_null, Integer expected) throws SQLException {
            String sql = String.format("SELECT %s(\'%s\' IN %s) AS result FROM data WHERE is_null = %s", func_name, substring, col_name, (is_null ? 1 : 0));

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            boolean exists = rs.next();
            assert(exists);
            if (is_null) {
                checkIntRow(rs, new String[]{"result"}, new int[1]);
            } else {
                checkIntRow(rs, new String[]{"result"}, new int[]{expected});
            }
            assertNoMoreRows(rs);
        }

    private void checkLikeOp(String col_name, String pattern, boolean is_null, Boolean expected) throws SQLException {
        // Test LIKE
        {
           String sql = String.format("SELECT %s LIKE ? AS result FROM data WHERE is_null = ?", col_name);
           PreparedStatement stmt = conn.prepareStatement(sql);
           stmt.setString(1, pattern);
           stmt.setInt(2, is_null ? 1 : 0);
           ResultSet rs = stmt.executeQuery();
           boolean exists = rs.next();
           assert(exists);
           if (is_null) {
               checkBooleanRow(rs, new String[]{"result"}, new Boolean[]{null});
           } else {
               checkBooleanRow(rs, new String[]{"result"}, new Boolean[]{expected});
           }
           assertNoMoreRows(rs);
        }

        // Test NOT LIKE
        {
            String sql = String.format("SELECT %s NOT LIKE ? AS result FROM data WHERE is_null = ?", col_name);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, pattern);
            stmt.setInt(2, is_null ? 1 : 0);
            ResultSet rs = stmt.executeQuery();
            boolean exists = rs.next();
            assert(exists);
            if (is_null) {
                checkBooleanRow(rs, new String[]{"result"}, new Boolean[]{null});
            } else {
                checkBooleanRow(rs, new String[]{"result"}, new Boolean[]{expected == null ? null : !expected});
            }
            assertNoMoreRows(rs);
        }
    }

    /**
     * Tests usage of trig udf functions
     * #744 test
     */
    @Test
    public void testCos() throws SQLException {
        checkDoubleFunc("cos", "double_val", false, 0.974487);
        checkDoubleFunc("cos", "double_val", true, null);
    }
    @Test
    public void testSin() throws SQLException {
        checkDoubleFunc("sin", "double_val", false, -0.224442);
        checkDoubleFunc("sin", "double_val", true, null);
    }
    @Test
    public void testTan() throws SQLException {
        checkDoubleFunc("tan", "double_val", false, -0.230318);
        checkDoubleFunc("tan", "double_val", true, null);
    }

    @Test
    public void testCosh() throws SQLException {
        checkDoubleFunc("cosh", "double_val", false, 114330.976031);
        checkDoubleFunc("cosh", "double_val", true, null);
    }

    @Test
    public void testSinh() throws SQLException {
        checkDoubleFunc("sinh", "double_val", false, 114330.976026);
        checkDoubleFunc("sinh", "double_val", true, null);
    }

    @Test
    public void testTanh() throws SQLException {
        checkDoubleFunc("tanh", "double_val", false, 1.000000);
        checkDoubleFunc("tanh", "double_val", true, null);
    }

    @Test
    public void testLog2() throws SQLException {
        checkDoubleFunc("log2", "double_val", false, 3.625270);
        checkDoubleFunc("log2", "double_val", true, null);
    }

    /**
     * String Functions
     */
    @Test
    public void testLower() throws SQLException {
        checkStringFunc("lower", "str_a_val", false, "abcdef");
        checkStringFunc("lower", "str_a_val", true, null);
    }

    @Test
    public void testPosition() throws SQLException {
        checkStringPositionFunc("position", "bC", "str_a_val", false, 2);
        checkStringPositionFunc("position", "bc", "str_a_val", false, 2);
        checkStringPositionFunc("position", "aa", "str_a_val", false, 0);
        checkStringPositionFunc("position", "bC", "str_a_val", true, null);
    }

    @Test
    public void testLike() throws SQLException {
        // Positive: Matches exact string when % and _ aren't used
        checkLikeOp("str_a_val", "AbCdEf", false, true);

        // Positive: % replaces zero or more characters
        checkLikeOp("str_a_val", "%", false, true);
        checkLikeOp("str_a_val", "AbCdEf%", false, true);
        checkLikeOp("str_a_val", "AbC%dEf", false, true);
        checkLikeOp("str_a_val", "Ab%", false, true);
        checkLikeOp("str_a_val", "%Ef", false, true);
        checkLikeOp("str_a_val", "%d%", false, true);
        checkLikeOp("str_a_val", "%A%d%E%", false, true);

        // Positive: _ replaces a single character
        checkLikeOp("str_a_val", "______", false, true);
        checkLikeOp("str_a_val", "_b____", false, true);
        checkLikeOp("str_a_val", "AbCd_f", false, true);
        checkLikeOp("str_a_val", "_bCdEf", false, true);
        checkLikeOp("str_a_val", "_b_d_f", false, true);

        // Positive: Combination of % and _
        checkLikeOp("str_a_val", "___%___", false, true);
        checkLikeOp("str_a_val", "_b%", false, true);
        checkLikeOp("str_a_val", "%d_f", false, true);
        checkLikeOp("str_a_val", "%C_E%", false, true);

        // Negative: random tests
        checkLikeOp("str_a_val", "", false, false);
        checkLikeOp("str_a_val", "_", false, false);
        checkLikeOp("str_a_val", "___%____", false, false);
        checkLikeOp("str_a_val", "AbCdEf_", false, false);
        checkLikeOp("str_a_val", "AbC", false, false);
        checkLikeOp("str_a_val", "\"AbCdEf\"", false, false);

        // Negative: LIKE is case sensitive
        checkLikeOp("str_a_val", "abcdef", false, false);
        checkLikeOp("str_a_val", "ABCDEF", false, false);
        checkLikeOp("str_a_val", "%e%", false, false);

        // Negative: Regex characters are escaped
        checkLikeOp("str_a_val", ".*", false, false);
        checkLikeOp("str_a_val", "\\D%", false, false);
        checkLikeOp("str_a_val", "(%)", false, false);
        checkLikeOp("str_a_val", "AbCd[E]f", false, false);

        // Null
        checkLikeOp("str_a_val", "abc", true, null);
        checkLikeOp("str_a_val", "AbCdEf", true, null);
    }
}
