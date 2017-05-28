
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


	public void start(Connection c) throws Exception{
		Scanner input = new Scanner(System.in);
		Boolean exit = false;
		conn = c;
		System.out.println("connection established");

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
						break;

					case "bookorders":
						break;

					case "topauthors":
						break;

					case "canship":
						break;

					default: 
						System.out.println("Unknown command: " + query);
				}
			}
		}
	}

/*	public String abbreviate(String str){
		if (str.length <= 25){
			return str;
		}
		else{
			String new_str = "";
			for (int i = 0; i < 22; i++){

			}
		}
	}*/

	public void orders(String pattern) throws Exception{
		PreparedStatement orders_stmt = conn.prepareStatement(ORDERS_QUERY);
		orders_stmt.setString(1, pattern);

		
		//SimpleDateFormat FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd");

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

			System.out.printf("Order: %10s \t %10s \t Customer: %20s \n", order_num, str_date, customer);
			System.out.println("Line ISBN \t Title \t \t \t Copies \t Price \t Total ");
			while (check){
				int line_num = res.getInt("linenumber");
				String isbn = res.getString("isbn");
				String title = res.getString("title");
				int copies = res.getInt("numcopies");
				double price = res.getDouble("bookprice");
				subtotal = subtotal + price*copies;

				System.out.printf("%d %-10s \t %20s \t %d \t %.2f \t %.2f \n", line_num, isbn, title, copies, price, price*copies);
				check = res.next();
			}
			System.out.printf("Subtotal: \t \t %.2f \n", subtotal);
			System.out.printf("Discount: \t \t %.2f \n", discount);
			System.out.printf("Total: \t \t %.2f \n", total);
		}
		else{
			System.out.println("Order number " + pattern + " not found.");
		}

	}

}
