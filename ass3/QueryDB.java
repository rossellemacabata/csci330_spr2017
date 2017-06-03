
import java.util.Scanner;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.lang.StringBuilder;

class QueryDB{
	static Connection conn = null;
	static final SimpleDateFormat FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd");

	static final String ORDERS_QUERY = "select ordernumber, orderdate, customername " +
				"from bookorder " +
				"where ordernumber like ? " +
				"order by orderdate, ordernumber;";

	static final String ORDER_QUERY = "select A.ordernumber, A.linenumber, A.numcopies, A.bookprice, B.isbn, B.title, C.customername, C.orderdate, C.totalamount, C.discount "+
				"from orderline as A " +
				"inner join book as B " +
					"on A.isbn = B.isbn " +
				"inner join bookorder as C " +
					"on A.ordernumber = C.ordernumber " +
				"where A.ordernumber = ? " +
				"order by A.linenumber;";


	static final String SHIPMENT_QUERY = "select B.ordernumber, A.shipmentid, A.shipmentdate, B.customername, B.orderdate, C.numcopies, C.isbn, D.title " +
				"from shipment as A " +
				"right outer join bookorder as B " +
					"on B.ordernumber = A.ordernumber " +
				"left outer join shipmentbook as C " +
					"on A.shipmentid = C.shipmentid " +
				"left outer join book as D " +
					"on C.isbn = D.isbn " +
				"where B.ordernumber = ?;";

	static final String BOOKORDERS_QUERY = "select distinct A.isbn, A.title, B.bookprice, B.numcopies, B.ordernumber, C.orderdate, C.customername, C.ordernumber, " +
				"group_concat(author.authorname separator ', ') as authors " +
				"from author " +
				"right outer join bookauthor " +
					"on author.authorid = bookauthor.authorid " +
				"left outer join macabar.book as A " +
					"on bookauthor.isbn = A.isbn " +
				"left outer join orderline as B " +
					"on B.isbn = A.isbn " +
				"left outer join bookorder as C " +
					"on C.ordernumber = B.ordernumber " +
				"where A.isbn = ? " +
				"group by B.numcopies, A.isbn, B.ordernumber " +
				"order by C.orderdate, C.ordernumber;";

	static final String TOPAUTHORS_QUERY = "select distinct A.authorname, A.authorid, sum(B.numcopies*B.bookprice) as total " +
				"from orderline as B " +
				"inner join bookauthor as C " +
					"on C.isbn = B.isbn " +
				"inner join author as A " +
					"on A.authorid = C.authorid " +
				"where C.authorid in ( " +
					"select C.authorid " +
    				"from bookauthor as D " +
    				"inner join book as C " +
    					"on C.isbn = D.isbn " +
    				"inner join orderline " +
    					"on orderline.isbn = C.isbn " +
    				"inner join author as A " +
    					"on D.authorid = A.authorid)" +
				"group by A.authorid, A.authorname " +
				"order by total desc, A.authorname limit 5;";

	static final String CANSHIP_QUERY = "select ordernumber, orderdate, customername, isbn, title, stock, shipped, canship " +
				"from ( " +
				"select ordernumber, orderdate, customername, orderline.isbn, orderline.numcopies as canship, " +
					"coalesce(shipmentbook.numcopies,0) as shipped " +
        			"from (bookorder natural join orderline) left join " + 
					"(shipment natural join shipmentbook) using (ordernumber,isbn) " +
					"order by orderline.isbn) as A " +
				"natural join book " + 
				"where stock > 0 and A.canship - A.shipped > 0 " +
				"order by orderdate, ordernumber;";

	public void start(Connection c) throws Exception{
		Scanner input = new Scanner(System.in);
		Boolean exit = false;
		conn = c;

		while (!exit && input.hasNextLine()){
			String[] command = input.nextLine().split("\\s+");
			if (command.length > 0){
				String query = command[0];

				switch (query){
					case "":
						break;

					case "quit":
						exit = true;
						break;

					case "order":
						order(command[1]);
						break;

					case "orders":
						orders(command[1]);
						break;

					case "shipment":
						shipment(command[1]);
						break;

					case "bookorders":
						bookorders(command[1]);
						break;

					case "topauthors":
						topauthors();
						break;

					case "canship":
						canship();
						break;

					default: 
						System.out.println("Unknown command: " + query);
				}
			}
		}
	}

	public String abbreviate(String str){
		if (str.length() <= 25){
			return str;
		}
		else{
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < 22; i++){
				char c = str.charAt(i);
				builder.append(c);
			}
			builder.append("...");
			String abbreviated = builder.toString();
			return abbreviated;
		}
	}

	public void orders(String pattern) throws Exception{
		PreparedStatement orders_stmt = conn.prepareStatement(ORDERS_QUERY);
		orders_stmt.setString(1, pattern);

		ResultSet res = orders_stmt.executeQuery();
		Boolean check = res.next();

		if (check){
			System.out.println("Order Number \t Date \t\t Customer");
			while (check){
				String order_num = res.getString("ordernumber");
				Date date = res.getDate("orderdate");
				String str_date = FORMAT.format(date);

				String customer = res.getString("customername");

				System.out.printf("%-10s \t %-10s \t %-30s\n", order_num, str_date, customer);
				check = res.next();
			}
		}
		else{
			System.out.println("No orders like " + pattern + " found.");
		}	
	}

	public void order(String pattern) throws Exception{
		PreparedStatement order_stmt = conn.prepareStatement(ORDER_QUERY);
		order_stmt.setString(1, pattern);

		ResultSet res = order_stmt.executeQuery();
		Boolean check = res.next();
		double subtotal = 0.0;

		if (check){
			String order_num = res.getString("ordernumber");
			Date date = res.getDate("orderdate");
			String str_date = FORMAT.format(date);
			String customer = res.getString("customername");

			double total = res.getDouble("totalamount");
			double discount = res.getDouble("discount");

			System.out.printf("Order: %10s \t %10s \t Customer: %s \n", order_num, str_date, customer);
			System.out.println("Line ISBN \t Title \t \t \t \t Copies  Price \t Total ");

			while (check){
				int line_num = res.getInt("linenumber");
				String isbn = res.getString("isbn");
				String title = res.getString("title");
				int copies = res.getInt("numcopies");
				double price = res.getDouble("bookprice");
				subtotal = subtotal + price*copies;

				System.out.printf("%5d %-10s %s \t %5d \t %-6.2f %6.2f \n", line_num, isbn, abbreviate(title), copies, price, price*copies);
				check = res.next();
			}
			System.out.printf("Subtotal: \t \t \t \t \t \t \t %-6.2f \n", subtotal);
			System.out.printf("Discount: \t \t \t \t \t \t \t %-6.2f \n", discount);
			System.out.printf("Total: \t \t \t \t \t \t \t \t %-6.2f \n", total);
		}
		else{
			System.out.println("Order number " + pattern + " not found.");
		}
	}

	public void shipment(String pattern) throws Exception{
		PreparedStatement shipment_stmt = conn.prepareStatement(SHIPMENT_QUERY);
		shipment_stmt.setString(1, pattern);

		ResultSet res = shipment_stmt.executeQuery();
		Boolean check = res.next();
		String prev = "";

		if (check){
			String order_num = res.getString("ordernumber");
			Date date1 = res.getDate("orderdate");
			String str_date = FORMAT.format(date1);
			String customer = res.getString("customername");

			System.out.printf("Order: %10s \t %10s \t Customer: %s \n", order_num, str_date, customer);
			String shipment_check = res.getString("shipmentid");

			if (shipment_check == null){
				System.out.println("No shipments.");
			}
			else{
				System.out.println("Shipment \t ShipDate \t Copies ISBN \t \t Title");

				while (check){
					String shipment_id = res.getString("shipmentid");
					Date date2 = res.getDate("shipmentdate");
					String ship_date = FORMAT.format(date2);
					int copies = res.getInt("numcopies");
					String isbn = res.getString("isbn");
					String title = res.getString("title");

					if (!checker(shipment_id,prev)){
						System.out.printf("%-8s \t %-10s \t %5d \t %10s \t%s \n", shipment_id, ship_date, copies, isbn, abbreviate(title));	
						prev = shipment_id;
					}
					else{
						System.out.printf("\t \t \t \t %5d \t %10s \t %s \n", copies, isbn, abbreviate(title));
					}
					check = res.next();
				}	
			}
		}
		else{
			System.out.println("Order number " + pattern + " not found");
		}
	}

	public void bookorders(String pattern) throws Exception{
		PreparedStatement bookorders_stmt = conn.prepareStatement(BOOKORDERS_QUERY);
		bookorders_stmt.setString(1, pattern);
		double total = 0.0;

		ResultSet res = bookorders_stmt.executeQuery();
		Boolean check = res.next();

		double subtotal = 0.0;

		if (check){
			String isbn = res.getString("isbn");
			String title = res.getString("title");
			String authors = res.getString("authors");
			System.out.printf("ISBN: %10s \t Title: %s \n", isbn, title);
			System.out.printf("\t \t \t Author: %s \n", authors);

			String order_check = res.getString("ordernumber");
			if (order_check == null){
				System.out.println("No orders");
			}
			else{
				System.out.println("OrderNumber \t Date \t \t Customer \t Copies Price \t Total");

				while (check){
					String order_num = res.getString("ordernumber");

					Date date = res.getDate("orderdate");
					String str_date = FORMAT.format(date);

					String customer = res.getString("customername");
					int copies = res.getInt("numcopies");
					double price = res.getDouble("bookprice");

					total = total + copies*price;

					System.out.printf("%-10s \t %-10s \t %s \t %5d \t %-6.2f %6.2f \n", order_num, str_date, customer, copies, price, copies*price);
					check = res.next();
				}
				System.out.printf("Total: \t \t \t \t \t \t \t \t %6.2f \n", total);
			}
		}
		else{
			System.out.println("No book " + pattern);
		}
	}

	public void topauthors() throws Exception{
		PreparedStatement topauthors_stmt = conn.prepareStatement(TOPAUTHORS_QUERY);
		ResultSet res = topauthors_stmt.executeQuery();
		Boolean check = res.next();

		if (check){
			System.out.println("AuthorID \t Name \t \t \t Total");
			while (check){
				String authorid = res.getString("authorid");
				String author_name = res.getString("authorname");
				double total_sales = res.getDouble("total");
				System.out.printf("%-10s \t %-20s \t  %-6.2f \n", authorid, author_name, total_sales);
				check = res.next();
			}
		}
		else{
			System.out.println("Error");
		}
	}

	public void canship() throws Exception{
		PreparedStatement canship_stmt = conn.prepareStatement(CANSHIP_QUERY);
		ResultSet res = canship_stmt.executeQuery();
		Boolean check = res.next();
		String prev = "";

		if (check){
			System.out.println("OrderNumber \t OrderDate \t Customer");
			System.out.println("\t ISBN \t \t Title \t \t \t \t Stock ToShip Shipped");
			while (check){
				String order_num = res.getString("ordernumber");
				Date date = res.getDate("orderdate");
				String str_date = FORMAT.format(date);
				String customer = res.getString("customername");
				

				String isbn = res.getString("isbn");
				String title = res.getString("title");

				int stock = res.getInt("stock");
				int can_ship = res.getInt("canship");
				int shipped = res.getInt("shipped");

				if (!checker(order_num,prev)){
					System.out.printf("%-10s \t %-10s %s \n", order_num, str_date, customer);
					prev = order_num;
				}
				
				System.out.printf("\t %-10s \t %s \t %5d \t %5d \t %5d \n", isbn, abbreviate(title), stock, can_ship, shipped);
				check = res.next();
			}
		}
		else{
			System.out.println("Error");
		}
	}

	public Boolean checker(String str1, String str2){
		if (str1.equals(str2)){
    		return true;
    	}
    	return false;
	}

}
