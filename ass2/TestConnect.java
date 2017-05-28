import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.util.Arrays;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.text.SimpleDateFormat;
import java.util.Date;


class TestConnect {
    static Connection conn = null;
    
    static final String DEFAULT_CONNECT_PARAMS = "connectparamsample.txt";
    static final String DEFAULT_BOOKS_PARAM = "books.txt";
    static final String DEFAULT_ORDERS_PARAM = "orders.txt";

    static final String DROP_TABLE = "drop table if exists atable;";
    static final String DROP_BOOK_TABLE = "drop table if exists book;";
    static final String DROP_AUTHOR_TABLE = "drop table if exists author;";
    static final String DROP_BOOKAUTHOR_TABLE = "drop table if exists bookauthor;";
    static final String DROP_BOOKORDER_TABLE = "drop table if exists bookorder;";
    static final String DROP_ORDERLINE_TABLE = "drop table if exists orderline;";
    static final String DROP_SHIPMENT_TABLE = "drop table if exists shipment;";
    static final String DROP_SHIPMENTBOOK_TABLE = "drop table if exists shipmentbook;";

     static final String BOOK_TABLE =
            "create table book(" +
                "isbn VARCHAR(13) not null," +
                "title VARCHAR(100) not null," +
                "bookprice NUMERIC(8,2) not null," +
                "stock INT not null," +
                "PRIMARY KEY (isbn)" +
            ");";

    static final String AUTHOR_TABLE =
            "create table author(" +
                "authorid VARCHAR(12) not null," +
                "authorname VARCHAR(100) not null," +
                "PRIMARY KEY (authorid)" +
            ");";

    static final String BOOKAUTHOR_TABLE =
            "create table bookauthor(" +
                "isbn VARCHAR(13) not null," +
                "authorid VARCHAR(12) not null," +
                "bookauthornumber INT not null," +
                "PRIMARY KEY (isbn, authorid)," +
                "FOREIGN KEY (isbn) REFERENCES book(isbn)," +
                "FOREIGN KEY (authorid) REFERENCES author(authorid)" +
            ");";

	
	static final String BOOKORDER_TABLE =
			"create table bookorder(" +
				"ordernumber VARCHAR(13) not null," +
				"customername VARCHAR(100)," + 
				"orderdate DATE not null," +
				"discount NUMERIC(8,2) not null," +
				"totalamount NUMERIC(8,2) not null," +
				"PRIMARY KEY (ordernumber)" +
			");";

	static final String ORDERLINE_TABLE = 
			"create table orderline(" +
				"ordernumber VARCHAR(13) not null," +
				"linenumber INT not null," +
				"isbn VARCHAR(13) not null," +
				"numcopies INT not null," + 
				"bookprice NUMERIC(8,2) not null," +
				"PRIMARY KEY (ordernumber, linenumber)," +
				"FOREIGN KEY (ordernumber) REFERENCES bookorder(ordernumber)," +
				"FOREIGN KEY (isbn) REFERENCES book(isbn)," +
				"UNIQUE (ordernumber, isbn)" +
			");";
	

	static final String SHIPMENT_TABLE =
			"create table shipment(" +
				"shipmentid VARCHAR(13) not null," +
				"ordernumber VARCHAR(13) not null," +
				"shipmentdate DATE not null," +
				"PRIMARY KEY (shipmentid)," +
				"FOREIGN KEY (ordernumber) REFERENCES bookorder(ordernumber)" +
			");";

	static final String SHIPMENTBOOK_TABLE =
			"create table shipmentbook(" +
				"shipmentid VARCHAR(13) not null," + 
				"isbn VARCHAR(13) not null," +
				"numcopies INT not null," +
				"PRIMARY KEY (shipmentid, isbn)," +
				"FOREIGN KEY (shipmentid) REFERENCES shipment(shipmentid)," +
				"FOREIGN KEY (isbn) REFERENCES book(isbn)" +
			");";

	static final String BOOKORDER_INSERT = "insert into bookorder (ordernumber, customername, orderdate, discount, totalamount)" +
    				"values (?, ?, ?, ?, ?);";

   	static final String ORDERLINE_INSERT = "insert into orderline (ordernumber, linenumber, isbn, numcopies, bookprice)" +
    				"values (?, ?, ?, ?, ?);";

    static final String BOOK_INSERT = "insert into book (isbn, title, bookprice, stock)" +
                    "values (?, ?, ?, ?);";

    static final String AUTHOR_INSERT = "insert into author (authorid, authorname)" +
        			"values (?, ?) on duplicate key update authorid = ?;";

    static final String BOOKAUTHOR_INSERT = "insert into bookauthor (isbn, authorid, bookauthornumber)" + 
       				"values (?, ?, ?);";


    static final String SHIPMENT_INSERT = "insert into shipment (shipmentid, ordernumber, shipmentdate)" +
    				"values (?, ?, ?) on duplicate key update ordernumber = ordernumber, shipmentdate = shipmentdate;";

   	static final String SHIPMENTBOOK_INSERT = "insert into shipmentbook (shipmentid, isbn, numcopies)" +
   					"values (?, ?, ?);";

   	static final String TOTAL_QUERY = "update bookorder " +
   					" set totalamount = ? " +
   					" where ordernumber = ?;";

	static final String CHECK_QUERY = "select totalamount " +
					" from bookorder;";

    public static void main(String[] args) throws Exception {
        String paramsFile = DEFAULT_CONNECT_PARAMS;
        String booksParam = DEFAULT_BOOKS_PARAM;
        String ordersParam = DEFAULT_ORDERS_PARAM;


        if (args.length == 1) {
            paramsFile = args[0];
        }

        if (args.length == 2){
        	paramsFile = args[0];
        	booksParam = args[1];
        }

        if (args.length == 3){
        	paramsFile = args[0];
        	booksParam = args[1];
        	ordersParam = args[3];
        }
        Properties connectprops = new Properties();
        connectprops.load(new FileInputStream(paramsFile)); 

        try {
            Class.forName("com.mysql.jdbc.Driver");
            String dburl = connectprops.getProperty("dburl");
            String username = connectprops.getProperty("user");
            conn = DriverManager.getConnection(dburl, connectprops);
            System.out.printf("Database connection %s %s established.%n",
                    dburl, username);
         
            createTables();
            
            File books_file = new File(booksParam);
           	File orders_file = new File(ordersParam);
            read_data(books_file, orders_file);
            
         
            conn.close();
        } catch (SQLException ex) {
            System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n",
                    ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
            // This line, if uncommented, will produce a stack trace
            ex.printStackTrace();
        }
    }
    
    static void createTables() throws SQLException {
        Statement stmt = conn.createStatement();

       	// Dropped the last time that I create first for every table
        stmt.execute(DROP_SHIPMENTBOOK_TABLE);
        stmt.execute(DROP_SHIPMENT_TABLE);
        stmt.execute(DROP_ORDERLINE_TABLE);
        stmt.execute(DROP_BOOKORDER_TABLE);
        stmt.execute(DROP_BOOKAUTHOR_TABLE);
        stmt.execute(DROP_BOOK_TABLE);
        stmt.execute(DROP_AUTHOR_TABLE);

        // Creating all tables
        stmt.execute(BOOK_TABLE);
        stmt.execute(AUTHOR_TABLE);
        stmt.execute(BOOKAUTHOR_TABLE);
        stmt.execute(BOOKORDER_TABLE);
      	stmt.execute(ORDERLINE_TABLE);
        stmt.execute(SHIPMENT_TABLE);
        stmt.execute(SHIPMENTBOOK_TABLE);
        stmt.close();
    }
    
    static void read_data(File file1, File file2) throws Exception{
        load_bookdata(file1);
        load_orderdata(file2);
    }

    // This function compares two strings
    // I use this to check if there is a new order 
    static boolean new_order(String str1, String str2){
    	if (str1.equals(str2)){
    		return true;
    	}
    	return false;
    }

    static void load_orderdata(File file) throws Exception{
    	String isbn = "";
    	String order_num = "";
    	String shipment_id = "";
    	String prev_order = "";

    	int num_copies = 0;
    	int linenumber = 1;

    	double book_price = 0.0;
    	double discount = 0.0;
    	double total = 0.0;
    	
   		Scanner line = new Scanner(file);
   		String header = line.nextLine();
    	
    	SimpleDateFormat FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd");
 		PreparedStatement bookorder_stmt = conn.prepareStatement(BOOKORDER_INSERT);
  		PreparedStatement orderline_stmt = conn.prepareStatement(ORDERLINE_INSERT);
  		PreparedStatement shipment_stmt = conn.prepareStatement(SHIPMENT_INSERT);
  		PreparedStatement shipmentbook_stmt = conn.prepareStatement(SHIPMENTBOOK_INSERT);
  		PreparedStatement update_stmt = conn.prepareStatement(TOTAL_QUERY);
		Statement check_stmt = conn.createStatement();

    	while (line.hasNextLine()){
    		String[] data = line.nextLine().split("\t");

			// Insert into bookorder table
    		if (!data[0].isEmpty()){
    			
    			java.util.Date date = FORMAT.parse(data[2]);
	    		java.sql.Date sql_date = new java.sql.Date(date.getTime());

	    		linenumber = 1;
	    		order_num = data[0];
	    		discount = Double.parseDouble(data[3]);
	    		
    			bookorder_stmt.setString(1, order_num);
	    		bookorder_stmt.setString(2, data[1]);
	    		bookorder_stmt.setDate(3, sql_date);
	    		bookorder_stmt.setDouble(4, discount);

	    		if (!new_order(prev_order, order_num)){
	    			bookorder_stmt.setDouble(5, 0.0);
    				bookorder_stmt.executeUpdate();
    				update_stmt.setDouble(1, total);
    				update_stmt.setString(2, prev_order);
    				update_stmt.executeUpdate();
    				total = 0.0 - discount;
    			}
    		}

    		// Insert into orderline table
    		if (!data[4].isEmpty()){
				isbn = data[4];
    			book_price = Double.parseDouble(data[6]);
    			num_copies = Integer.parseInt(data[5]);

    			orderline_stmt.setString(1, order_num);
    			orderline_stmt.setInt(2, linenumber);
    			orderline_stmt.setString(3, isbn);
    			orderline_stmt.setInt(4, num_copies);
    			orderline_stmt.setDouble(5, book_price);
				
    			orderline_stmt.executeUpdate();
				total += num_copies*book_price;
    			linenumber++;
    		}

    		// Insert into shipment table
  			if (data.length > 7 && !data[7].isEmpty()){
  				java.util.Date date = FORMAT.parse(data[8]);
		    	java.sql.Date sql_date = new java.sql.Date(date.getTime());

  				shipment_id = data[7];
  				shipment_stmt.setString(1, shipment_id);
  				shipment_stmt.setString(2, order_num);
  				shipment_stmt.setDate(3, sql_date);
  				shipment_stmt.executeUpdate();

  				shipmentbook_stmt.setString(1, shipment_id);
  				shipmentbook_stmt.setString(2, isbn);
  				shipmentbook_stmt.setInt(3, Integer.parseInt(data[9]));
  				shipmentbook_stmt.executeUpdate();
  			}
 			prev_order = order_num;
    	}
    	update_stmt.setDouble(1, total);
    	update_stmt.setString(2, prev_order);
    	update_stmt.executeUpdate();

    	bookorder_stmt.close();
    	orderline_stmt.close();
    	shipment_stmt.close();
    	shipmentbook_stmt.close();
    }


    static void load_bookdata(File file) throws Exception{
        Scanner line = new Scanner(file);
        String header = line.nextLine();
        while (line.hasNextLine()){

        	String[] line_array = line.nextLine().split("\t");
            PreparedStatement book_stmt = conn.prepareStatement(BOOK_INSERT);
            PreparedStatement author_stmt = conn.prepareStatement(AUTHOR_INSERT);
            PreparedStatement bookauthor_stmt = conn.prepareStatement(BOOKAUTHOR_INSERT);

            // Insert into book table
            book_stmt.setString(1, line_array[0]);
            book_stmt.setString(2, line_array[1]);
            book_stmt.setDouble(3, Double.parseDouble(line_array[2]));
            book_stmt.setInt(4, Integer.parseInt(line_array[3]));
            book_stmt.executeUpdate();
            book_stmt.close();

            // Insert into author table
			for (int i = 4; i < line_array.length; i+=2){
				author_stmt.setString(1, line_array[i]);
				author_stmt.setString(2, line_array[i+1]);
				author_stmt.setString(3, line_array[i]);
				author_stmt.executeUpdate();
            }
            author_stmt.close();


            // Insert into book author
            bookauthor_stmt.setString(1, line_array[0]);
            int bookauthor_num = 0;
            for (int j = 4; j < line_array.length; j+=2){
            	bookauthor_stmt.setString(2, line_array[j]);
            	bookauthor_num++;
            	bookauthor_stmt.setInt(3, bookauthor_num);
            	bookauthor_stmt.executeUpdate();
            }
            bookauthor_stmt.close();
           
        }     
    }
}
