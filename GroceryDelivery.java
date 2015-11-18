//CS 1555 Term Project
//Fall 2015
//David Neiman, Xinyue Huang, Shijia Liu

import java.sql.*;  //import the file containing definitions for the parts
import java.text.ParseException;
                    //needed by java for database connection and manipulation
import java.sql.Date;
import java.util.*;

public class GroceryDelivery {
    private static Connection connection; //used to hold the jdbc connection to the DB
    private Statement statement; //used to create an instance of the connection
    private PreparedStatement preparedStatement; //used to create a prepared statement, that will be later reused
    private ResultSet resultSet; //used to hold the result of your query (if one
    // exists)
    private String query;  //this will hold the query we are using
    
    public GroceryDelivery()
    {
    	createTables();
        populateTables();
    }

    public void populateTables(){
    
    	try 
    	{
    		Scanner scan = new Scanner(System.in);
    		int numberDistribution, numberCustomers, numberItems, maxOrdersPerCustomer, maxLineItemsPerOrder;
    		
    		//getting user input
    		
    		System.out.print("Enter how many distribution centers you want for warehouse #1: ");
    		try {
    			numberDistribution = Integer.parseInt(scan.next());
    			
    			if (numberDistribution < 1)
    			{
    				System.out.println("Not a valid entry: Defaulting to 10.");
    				numberDistribution = 10;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println("Not a valid entry: Defaulting to 10.");
    			numberDistribution = 10;
    		}
    		
    		System.out.print("Enter how many customers you want total: ");
    		try {
    			numberCustomers = Integer.parseInt(scan.next());
    			
    			if (numberCustomers < 1)
    			{
    				System.out.println("Not a valid entry: Defaulting to 100.");
    				numberCustomers = 100;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println("Not a valid entry: Defaulting to 100.");
    			numberCustomers = 100;
    		}
    		
    		System.out.print("Enter how many items you want total: ");
    		try {
    			numberItems = Integer.parseInt(scan.next());
    			if (numberItems < 1)
    			{
    				System.out.println("Not a valid entry: Defaulting to 25.");
    				numberItems = 25;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println("Not a valid entry: Defaulting to 25.");
    			numberItems = 25;
    		}
    		
    		System.out.print("The number of orders for each customer is randomly determined individually, with every customer having at least one order. Enter the maximum number possible: ");
    		try {
    			maxOrdersPerCustomer = Integer.parseInt(scan.next());
    			
    			if (maxOrdersPerCustomer < 1)
    			{
    				System.out.println("Not a valid entry: Defaulting to 5.");
    				maxOrdersPerCustomer = 5;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println("Not a valid entry: Defaulting to 5.");
    			maxOrdersPerCustomer = 5;
    		}
    		
    		System.out.print("The number of line items in each order is randomly determined individually, with every order having at least one line item. Enter the maximum number possible: ");
    		try {
    			maxLineItemsPerOrder = Integer.parseInt(scan.next());
    			if (maxLineItemsPerOrder < 1)
    			{
    				System.out.println("Not a valid entry: Defaulting to 5.");
    				maxLineItemsPerOrder = 5;
    			}
    			//can only use each once
    			if (maxLineItemsPerOrder > numberItems)
    			{
    				System.out.println("Too many items: Setting to " + numberItems + ".");
    				maxLineItemsPerOrder = numberItems;
    			}
    		}
    		catch (Exception e)
    		{
    			System.out.println("Not a valid entry: Defaulting to 5.");
    			maxLineItemsPerOrder = 5;
    		}
    	
    		System.out.println("\nPopulating table now...");
    		System.out.println("---------------------------------------------\n");
    	 
			int numberWarehouses = 1;
			
    		String startTransaction = "SET TRANSACTION READ WRITE";
        	String insertWarehouses = "insert into warehouses values(1, 'GrowRoom', '1008 Ross Park Mall Dr', 'Pittsburgh', 'PA', '15237', 0.07, 1000000)";
       		statement = connection.createStatement();
       		System.out.println("Connection established successfully.");
       		
       		//inserting into warehouses table
       		statement.executeUpdate(startTransaction);
       		System.out.println("Transaction started successfully.\n");
       		statement.executeUpdate(insertWarehouses);
			//System.out.println(insertWarehouses);
			System.out.println("Inserted 1 tuple into the warehouses table.");
			Random r = new Random();
		
			//inserting into distribution_station table
			for (int i = 0; i < numberDistribution; i++)
			{
				int warehouse_number = 1;
				int id = i + 1;
				String name = "name" + i;
				String address = "address" + i;
				String city = "city" + i;
				String state = "state" + i;
				String zip = (15000 + i) + "";
				double tax_rate = r.nextDouble()*40;
				double sales_sum = r.nextDouble()*10000;
				
				String insertDistribution = "insert into distribution_station values(" + 
				warehouse_number + ", " + 		
				id + ", " + 					
				"'" + name + "', " + 			
				"'" + address + "', " + 		
				"'" + city + "', " + 			
				"'" + state + "', " + 			
				"'" + zip + "', " + 			
				tax_rate + ", " +   			
				sales_sum + ")";				
				
				//System.out.println(insertDistribution);
				statement.executeUpdate(insertDistribution);
				
			}
			System.out.println("Inserted " + numberDistribution + " tuples into the distribution_station table.");
				
		
    		//insert into items        
            for (int i = 0; i < numberItems; i++)
            {
            	int id = i + 1;
            	String name = "item" + i;
            	double price = Math.round(r.nextDouble()*100*100)/100;
            	
            	String insertItem = "insert into items values(" +
            	id + ", " +
            	"'" + name + "', " +
            	price + ")";
            	
            	//System.out.println(insertItem);
            	statement.executeUpdate(insertItem);
				
            }
            System.out.println("Inserted " + numberItems + " tuples into the items table.");
			
			int lastID = 0;
			
			// insert into customers
			for (int i = 0; i < numberCustomers; i++)
			{
				//number of customers per distribution center
				int number = numberCustomers/numberDistribution; 
				
				//if more customers than distribution centers, default = 1 per center
				if (number == 0)
					number = 1;
				
				int warehouse_id = 1;
				int distributor_id = (i/number) + 1;
				int id = (i%number) + 1;
				
				//if number of distribution centers does not divide numberCustomers
				//evenly, need to check at the upper bound
				//ie 100 cust, 12 centers. 
				//So instead of 8 people for last center, will have all the rest (16
				
				if (distributor_id > numberDistribution)
				{
					distributor_id = numberDistribution;
					id = lastID + 1;
				}
				lastID = id;
					
				String fname = "aaa" + i;
				String middle_init = "a" + i;
				String lname = "zzz" + i;
				String address = i + " Pittsburgh St.";
				String city = "city" + i;
				String state = "state" + i;
				String zip = (15000 + i) + "";
				String phone = "phone" + i;
				int month = r.nextInt(12) + 1;
				int day = r.nextInt(28) + 1;
				int year = r.nextInt(6) + 2010;
				String initial_date = "TO_DATE('" + month + "-" + day + "-" + year + "', 'MM-DD-YYYY')";
				double discount = r.nextDouble()*100;
				double outstanding_balance = r.nextDouble()*1000;
				double year_spend = r.nextDouble()*1000 + outstanding_balance;
				int number_payments = r.nextInt(20);
				int number_deliveries = r.nextInt(20);
			
				String insertCustomer = "insert into customers values(" +
				warehouse_id + ", " + 
				distributor_id + ", " + 
				id + ", " + 
				"'" + fname + "', " + 
				"'" + middle_init + "', " +
				"'" + lname + "', " +
				"'" + address + "', " +
				"'" + city + "', " +
				"'" + state + "', " +
				"'" + zip + "', " +
				"'" + phone + "', " +
				initial_date + ", " +
				discount + ", " + 
				outstanding_balance + ", " + 
				year_spend + ", " + 
				number_payments + ", " +
				number_deliveries + ")";
				
				//System.out.println(insertCustomer);
				statement.executeUpdate(insertCustomer);
				
			}
			System.out.println("Inserted " + numberCustomers + " tuples into the customers table.");
			
			//Number is dependent on random outcome, so will keep track.
			int numOrdersInserted = 0;
			int numLineItemsInserted = 0;
			int lastI = 0;
			lastID = numberCustomers/numberDistribution;
		
			//insert into orders, lineItems table
			for (int i = 0; i < numberCustomers; i++)
			{
				int numOrders = r.nextInt(maxOrdersPerCustomer) + 1;
				
				for (int j = 0; j < numOrders; j++)
				{
					//number of customers per distribution center
					int number = numberCustomers/numberDistribution;
					
					//if more customers than distribution centers, default = 1 per center
					if (number == 0)
						number = 1;
				
					int warehouse_id = 1;
					int distributor_id = (i/number) + 1;
					int custID = (i%number) + 1;
					int orderID = j + 1;
				
					//if number of distribution centers does not divide numberCustomers
					//evenly, need to check at the upper bound
					//ie 100 cust, 12 centers. 
					//So instead of 8 people for last center, will have all the rest
					// (12,12,12,12,12,12,12,16)
					
					//System.out.println("\ni+1 = " + (i+1));
				
					if (distributor_id > numberDistribution)
					{
						distributor_id = numberDistribution;
						
						//this is the first order. Need to switch custID
						if (lastI != i)
						{
							custID = lastID + 1;
							lastID = custID;
						}
						// just need to use current custID
						else
						{
							custID = lastID;
						}
					}
					lastI = i;
					
					int month = r.nextInt(12) + 1;
					int day = r.nextInt(28) + 1;
					int year = r.nextInt(6) + 2010;
					String order_date = "TO_DATE('" + month + "-" + day + "-" + year + "', 'MM-DD-YYYY')";
				
					//int completed = r.nextInt(2);
					int completed = 1;
					int num_lineItems = r.nextInt(maxLineItemsPerOrder)+1;
				
					String insertOrder = "insert into orders values(" +
					warehouse_id + ", " + 
					distributor_id + ", " + 
					custID + ", " + 
					orderID + ", " +
					order_date + ", " +
					completed + ", " +
					num_lineItems + ")";
					
					numOrdersInserted++;
				
					//System.out.println(insertOrder);
					try {
						statement.executeUpdate(insertOrder);
					}
					catch (Exception e)
					{
						System.out.println("Failed here:");
						System.out.println(insertOrder);
					}
					
					//ArrayList to store which itemIDs were already used. 
					ArrayList <Integer> entered = new ArrayList <Integer> ();
            		
            		for (int x = 0; x < num_lineItems; x++)
            		{
            			int lineItemID = x+1;
            			int itemID = r.nextInt(numberItems)+1;
            			
            			// if duplicate, repick
            			// Will violate PK if itemID already was used
            			while (entered.contains(itemID))
            				itemID = r.nextInt(numberItems)+1;
            			
            			entered.add(itemID);
            				
            			int quantity = r.nextInt(10)+1;
            			double price = Math.round(quantity * r.nextDouble()*150*100)/100;
            			int monthDeliv = r.nextInt(12) + 1;
						int dayDeliv = r.nextInt(28) + 1;
						int yearDeliv = r.nextInt(6) + 2010;
						String date_delivered = "TO_DATE('" + monthDeliv + "-" + dayDeliv + "-" + yearDeliv + "', 'MM-DD-YYYY')";
            		
            			String insertLineItem = "insert into LineItems values(" + 
            			warehouse_id + ", " + 
						distributor_id + ", " + 
						custID + ", " + 
						orderID + ", " +
						lineItemID + ", " +
						itemID + ", " +
						quantity + ", " +
						price + ", " +
						date_delivered + ")";
						//System.out.println(insertLineItem);
						try {
							statement.executeUpdate(insertLineItem);
						}
						catch (Exception e)
						{
							System.out.println("Failed here:");
							System.out.println(insertLineItem);
						}
						numLineItemsInserted++;
						
					}
				
				}
			}
			
			//numLineItems
			System.out.println("Inserted " + numOrdersInserted + " tuples into the orders table.");
			System.out.println("Inserted " + numLineItemsInserted + " tuples into the lineItems table.");
			
			for (int i = 0; i < numberItems; i++)
			{    	
            	int warehouse_id = 1;
            	int item_id = i + 1;
            	int inStock = r.nextInt(1000000);
            	int sold = r.nextInt(1000000);
            	int orders = r.nextInt(1000000);
            	
            	String insertWarehouseStock = "insert into warehouse_stock values(" +
            	warehouse_id + ", " + 
            	item_id + ", " +
            	inStock + ", " +
            	sold + ", " +
            	orders + ")";
            	
            	//System.out.println(insertWarehouseStock);
				statement.executeUpdate(insertWarehouseStock);
            	
			}
			
			int numberWarehouseStockInserted = numberWarehouses * numberItems;
            System.out.println("Inserted " + numberWarehouseStockInserted + " tuples into the warehouse_stock table.\n");
            
            statement.executeUpdate("COMMIT");
            System.out.println("Transaction committed.\n");
            
        } catch(SQLException Ex) {
            System.out.println("Error running the queries.  Machine Error: " +
                    Ex.toString());
        } finally{
            try {
                if (statement != null)
                    statement.close();
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException e) {
                System.out.println("Cannot close Statement. Machine error: "+e.toString());
            }
        }
    
    

    }
    
    public void createTables()
    {
    	try {
       		System.out.println("\nInitializing database for Class registration.");
       		System.out.println("---------------------------------------------\n");
       		String startTransaction = "SET TRANSACTION READ WRITE";
        	
        	String dropTableWarehouses = "drop table warehouses cascade constraints";
       		String createTableWarehouses = "create table warehouses (" + 
           		"id number(10)," + 
           		"name varchar2(20)," + 
        		"address varchar2(40)," + 
            	"city varchar2(15)," + 
        		"state varchar2(15)," +   
            	"zip varchar2(10)," +  
            	"tax_rate number(6,2) check (tax_rate > 0)," +    
            	"sales_sum number(15,2) default 0," +  
           		"primary key(id) )";
            String dropTableDistributionStation = "drop table distribution_station cascade constraints";
            String createTableDistributionStation = "create table distribution_station (" + 
            	"warehouse_id number(10)," +
            	"id number(10)," +
            	"name varchar2(20)," +
            	"address varchar2(40)," +
            	"city varchar2(15)," +
            	"state varchar2(15)," +
            	"zip varchar2(10)," +
            	"tax_rate number(6,2) check(tax_rate>0)," +
            	"sales_sum number(15,2) default 0," +
            	"primary key (warehouse_id, id)," +
            	"foreign key (warehouse_id) references warehouses(id) deferrable initially deferred)";
            String dropTableCustomers = "drop table customers cascade constraints";
            String createTableCustomers = "create table customers (" +
		        "warehouse_id number(10)," +
            	"distributor_id number(10)," +
            	"id number(10)," +
            	"fname varchar2(20)," +
            	"middle_init varchar2(5)," +
            	"lname varchar2(20)," +
            	"address varchar2(40)," +
            	"city varchar2(40)," +
            	"state varchar2(15)," +
            	"zip varchar2(10)," +
            	"phone varchar2(15)," +
            	"initial_date date," +
            	"discount number(4,2)," +
            	"outstanding_balance number(10,2)," +
            	"year_spend number(10,2)," +
            	"number_payments number(10)," +
            	"num_deliveries number(10)," +
            	"primary key(warehouse_id, distributor_id, id)," +
            	"foreign key(warehouse_id, distributor_id) references distribution_station(warehouse_id, id) deferrable initially deferred)";
            String dropTableOrders = "drop table orders cascade constraints";
            String createTableOrders = "create table orders (" +
                "warehouse_id number(10)," +
                "distributor_id number(10)," +
            	"custID number(10)," +
            	"id number(15)," +
            	"order_date date," +
            	"completed char check(completed in(0,1))," +
            	"num_lineItems number(5)," +
            	"primary key(warehouse_id, distributor_id, custID, id)," +
            	"foreign key(warehouse_id, distributor_id, custID) references customers(warehouse_id, distributor_id, id) deferrable initially deferred)";
            String dropTableLineItems = "drop table LineItems cascade constraints";
            String createTableLineItems = "create table LineItems(" +
                "warehouse_id number(10)," +
                "distributor_id number(10)," +
		        "custID number(10)," +
            	"order_id number(15)," +
            	"id number(10)," +
            	"item_id number(10)," +
            	"quantity number(10)," +
            	"price number(10,2)," +
            	"date_delivered date," +
            	"primary key(warehouse_id, distributor_id, custID, order_id, id),"+
		        "foreign key(warehouse_id, distributor_id, custID, order_id) references orders(warehouse_id, distributor_id, custID, id) deferrable initially deferred," +
            	"foreign key(item_id) references items(id) deferrable initially deferred)";
            String dropTableItems = "drop table items cascade constraints";
            String createTableItems = "create table items(" +
            	"id number(10)," +
            	"name varchar2(20)," +
            	"price number(10,2)," +
            	"primary key(id))";
            String dropTableWarehouseStock = "drop table warehouse_stock cascade constraints";
            String createTableWarehouseStock = "create table warehouse_stock(" +
            	"warehouse_id number(10)," +
            	"item_id number(10)," +
            	"quantity_in_stock number(10)," +
            	"quantity_sold number(10)," +
            	"number_orders number(15)," +
            	"primary key(warehouse_id, item_id)," +
            	"foreign key(warehouse_id) references warehouses(id) deferrable initially deferred," +
            	"foreign key(item_id) references items(id) deferrable initially deferred)";
            	
            statement = connection.createStatement();
            System.out.println("Connection established successfully.");
            //statement.executeUpdate(startTransaction);
            System.out.println("Transaction started successfully.\n");
            try {
            statement.executeUpdate(dropTableWarehouses);
            } 
            catch (Exception e) 
            {}
            statement.executeUpdate(createTableWarehouses);
            System.out.println("Table warehouses created successfully.");
            try {
            	statement.executeUpdate(dropTableDistributionStation);
            }
            catch (Exception e)
            {}
            statement.executeUpdate(createTableDistributionStation);
            System.out.println("Table distribution_station created successfully.");
            
            try {
            	statement.executeUpdate(dropTableCustomers);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableCustomers);
            System.out.println("Table customers created successfully.");
            
            try {
            	statement.executeUpdate(dropTableOrders);
            }
            catch (Exception e)
            {}
            statement.executeUpdate(createTableOrders);
            System.out.println("Table orders created successfully.");
            
            try {
            	statement.executeUpdate(dropTableLineItems);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableLineItems);
            System.out.println("Table lineItems created successfully.");
            
            try {
            	statement.executeUpdate(dropTableItems);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableItems);
            
            System.out.println("Table items created successfully.");
            
            try {
            	statement.executeUpdate(dropTableWarehouseStock);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableWarehouseStock);
            System.out.println("Table warehouse_stock created successfully.\n");
            
            statement.executeUpdate("COMMIT");
            System.out.println("Transaction committed.\n");
        } catch(SQLException Ex) {
            System.out.println("Error creating the queries.  Machine Error: " +
                    Ex.toString());
        } finally{
            try {
                if (statement != null)
                    statement.close();
                if (preparedStatement != null)
                    preparedStatement.close();
            } catch (SQLException e) {
                System.out.println("Cannot close Statement. Machine error: "+e.toString());
            }
        }
    }
    
	public static void main(String args[]) throws SQLException, ClassNotFoundException
	{
    	/* Making a connection to a DB causes certain exceptions.  In order to handle
	   these, you either put the DB stuff in a try block or have your function
	   throw the Exceptions and handle them later.  For this demo I will use the
	   try blocks */

		String username, password;
		username = "dsn9"; //This is your username in oracle
		password = "3766567"; //This is your password in oracle
	
		try{
	   		// Register the oracle driver.  
	    	DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
	    
	    	//This is the location of the database.  This is the database in oracle
	    	//provided to the class
	    	String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass"; 
	    
	    	//create a connection to DB on class3.cs.pitt.edu
	    	connection = DriverManager.getConnection(url, username, password);     
	    	GroceryDelivery test = new GroceryDelivery();
		}	
		catch(SQLException Ex)  {
	    	System.out.println("Error connecting to database.  Machine Error: " +
			       	Ex.toString());
		}
		finally
		{
			/*
			 * NOTE: the connection should be created once and used through out the whole project;
			 * Is very expensive to open a connection therefore you should not close it after every operation on database
			 */
			connection.close();
		}
  	}
}
