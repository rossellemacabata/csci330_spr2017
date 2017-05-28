import java.io.FileNotFoundException;
import java.util.Properties;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Connect{

    static Connection conn = null;
    static final String DEFAULT_CONNECT_PARAMS = "connectparamsample.txt";
    static final String DROP_TABLE = "drop table if exists atable;";
    static final String CREATE_TABLE =
            "create table atable(" +
                    " col1 VARCHAR(20)," +
                    " col2 INT);";
    static final String INSERT_DATA = "insert into atable (col1,col2) values (?,?);";
    static final String QUERY_TABLE = "select col1, col2 from atable where col2 = ?;";

    static final String BOOK_TABLE =
            "create table book(" +
                    "isbn VARCHAR(13) not null," +
                    "title VARCHAR(100) not null," +
                    "bookprice NUMERIC(8,2) not null," +
                    "stock INT not null);";

    static final String AUTHOR_TABLE =
            "create table author(" +
                    "authorid VARCHAR(12) not null," +
                    "authorname VARCHAR(100) not null);";


    public static void main(String[] args) throws Exception{
        String paramsFile = DEFAULT_CONNECT_PARAMS;

        if (args.length >= 1) {
            paramsFile = args[0];
        }

        Properties connectProps = new Properties();

        connectProps.load(new FileInputStream(paramsFile));


        try {
            
            Class.forName("com.mysql.jdbs.Driver");
            String dburl = connectProps.getProperty("dburl");
            String username = connectProps.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectProps);
            System.out.printf("Database connection %s %s established.%n", dburl, username);

            createTable();
            loadTable();
            showTable(1);
            showTable(2);
            showTable(3);

            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());

            ex.printStackTrace();
        } catch (FileNotFoundException ex) {

        }
    }


    static void createTable() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(DROP_TABLE);
        stmt.execute(CREATE_TABLE);
        stmt.close();
    }

    static void loadTable() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(INSERT_DATA);
        stmt.setString(1, "First line");
        stmt.setInt(2, 1);
        stmt.executeUpdate();
        stmt.setString(1, "Second line");
        stmt.setInt(2, 2);
        stmt.executeUpdate();
        stmt.setString(1, "Third line");
        stmt.executeUpdate();
        stmt.close();
    }

    static void showTable(int value) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(QUERY_TABLE);
        stmt.setInt(1, value);
        ResultSet results = stmt.executeQuery();

        System.out.printf("showTable(%d)%n", value);
        int nrows = 0;
        while (results.next()) {
            System.out.printf("col1: %s, col2: %d%n",
                    results.getString(1), results.getInt(2));
            nrows += 1;
        }
        System.out.printf("%d rows returned%n", nrows);
        stmt.close();
    }
}