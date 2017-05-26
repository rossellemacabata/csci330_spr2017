class ReadData{
	
	static void load_orderdata(File file) throws Exception{
   		Scanner line = new Scanner(file);
   		String header = line.nextLine();
    	
    	SimpleDateFormat FORMAT = new java.text.SimpleDateFormat("yyyy-MM-dd");
 		PreparedStatement bookorder_stmt = conn.prepareStatement(BOOKORDER_INSERT);
  		PreparedStatement orderline_stmt = conn.prepareStatement(ORDERLINE_INSERT);
  		PreparedStatement shipment_stmt = conn.prepareStatement(SHIPMENT_INSERT);
  		PreparedStatement shipmentbook_stmt = conn.prepareStatement(SHIPMENTBOOK_INSERT);

    	String ISBN = "";
    	int NUM_COPIES = 0;
    	double BOOK_PRICE = 0.0;
    	String ORDER_NUM = "";
    	double DISCOUNT = 0.0;
    	String SHIPMENT_ID = "";

    	double total = 0.0;
    	int linenumber = 1;

    	while (line.hasNextLine()){
    		String[] data = line.nextLine().split("\t");


    		if (!data[0].isEmpty()){
    			java.util.Date date = FORMAT.parse(data[2]);
	    		java.sql.Date sql_date = new java.sql.Date(date.getTime());

	    		linenumber = 1;
	    		ORDER_NUM = data[0];
	    		DISCOUNT = Double.parseDouble(data[3]);
	    		//total = total - DISCOUNT;
    			bookorder_stmt.setString(1, ORDER_NUM);
	    		bookorder_stmt.setString(2, data[1]);
	    		bookorder_stmt.setDate(3, sql_date);
	    		bookorder_stmt.setDouble(4, DISCOUNT);
	    		bookorder_stmt.setDouble(5, total);
	    		bookorder_stmt.executeUpdate();
	    		System.out.println("Bookorder " + ORDER_NUM + "\n");

    		}
    		if (!data[4].isEmpty()){
				ISBN = data[4];
    			BOOK_PRICE = Double.parseDouble(data[6]);
    			NUM_COPIES = Integer.parseInt(data[5]);
    			orderline_stmt.setString(1, ORDER_NUM);
    			orderline_stmt.setInt(2, linenumber);
    			orderline_stmt.setString(3, ISBN);
    			orderline_stmt.setInt(4, NUM_COPIES);
    			orderline_stmt.setDouble(5, BOOK_PRICE);
				
    			orderline_stmt.executeUpdate();
				//total += NUM_COPIES*BOOK_PRICE;
    			linenumber++;
    			System.out.println("Line order " +ORDER_NUM + "\n");
    		}

    		//System.out.println(data[7]);
  			if (!data[7].isEmpty()){
  				System.out.println(data[7]);
  				java.util.Date date = FORMAT.parse(data[8]);
		    	java.sql.Date sql_date = new java.sql.Date(date.getTime());

  				SHIPMENT_ID = data[7];
  				shipment_stmt.setString(1, SHIPMENT_ID);
  				shipment_stmt.setString(2, ORDER_NUM);
  				shipment_stmt.setDate(3, sql_date);
  				shipment_stmt.executeUpdate();
  				System.out.println("Shipment " + ORDER_NUM + "\n");
  			}


  			System.out.println("asdjfhjkdsfh");

 
    	}
    	System.out.println("exit");
    	bookorder_stmt.close();
    	orderline_stmt.close();
    	shipment_stmt.close();
    	shipmentbook_stmt.close();

    }


    static void load_bookdata(File file) throws Exception{
        //String BOOK_INSERT = "";
       // String AUTHOR_INSERT = "";
        //String CHECK_INSERT = "";
       // String BOOKAUTHOR_INSERT = "";

        Scanner line = new Scanner(file);
        String header = line.nextLine();
        while (line.hasNextLine()){
        //try{
        	String[] line_array = line.nextLine().split("\t");
            PreparedStatement book_stmt = conn.prepareStatement(BOOK_INSERT);
            PreparedStatement author_stmt = conn.prepareStatement(AUTHOR_INSERT);
            PreparedStatement bookauthor_stmt = conn.prepareStatement(BOOKAUTHOR_INSERT);

            // INSERT INTO BOOK TABLE
            book_stmt.setString(1, line_array[0]);
            book_stmt.setString(2, line_array[1]);
            book_stmt.setDouble(3, Double.parseDouble(line_array[2]));
            book_stmt.setInt(4, Integer.parseInt(line_array[3]));
            book_stmt.executeUpdate();
            book_stmt.close();

            // INSERT INTO AUTHOR TABLE
			for (int i = 4; i < line_array.length; i+=2){
				author_stmt.setString(1, line_array[i]);
				author_stmt.setString(2, line_array[i+1]);
				author_stmt.setString(3, line_array[i]);
				author_stmt.executeUpdate();
            }
            author_stmt.close();


            // INSERT INTO BOOKAUTHOR TABLE
            bookauthor_stmt.setString(1, line_array[0]);
            int bookauthor_num = 0;
            for (int j = 4; j < line_array.length; j+=2){
            	bookauthor_stmt.setString(2, line_array[j]);
            	bookauthor_num++;
            	bookauthor_stmt.setInt(3, bookauthor_num);
            	bookauthor_stmt.executeUpdate();
            }
            bookauthor_stmt.close();
            
      //  } catch (SQLException ex){
       // 	System.out.println("Error: " + ex);
        }
       // }        
    }


}