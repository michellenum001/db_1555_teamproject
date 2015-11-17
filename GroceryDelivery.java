//CS 1555 Term Project
//Fall 2015

import java.sql.*;  //import the file containing definitions for the parts
import java.text.ParseException;
                    //needed by java for database connection and manipulation
import java.sql.Date;

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
        String insertWarehouses = "insert into warehouses values(1, 'GrowRoom', '1008 Ross Park Mall Dr'
            , 'Pittsburgh', 'PA', 15237, 0.07, 1000000)";

        String[] distriName = {"distri_one", "distri_two", "distri_three", "distri_four", "distri_five"};
        String[] distriAddress = {"6425 Penn Ave #700", "5000 Forbes Ave", "6001 University Blvd", 
                                "1 Waterfront Pl", "840 Wood St"};
        String[] distriCity = {"Pittsburgh", "Pittsburgh", "Moon", "Morgantown", "Clarion"};
        String[] distriState = {"PA", "PA", "PA", "MV", "CA"};
        String[] distriZip = {"15206", "15213", "15408", "13273", "15678"};
        String[] distriTax = {"0.07", "0.07", "0.07", "0.08", "0.09"};
        String[] distriSales = {"220000", "320000", "445000", "234000", "126000"};

    }
    
    public void createTables()
    {
    	try {
       		System.out.println("Initializing database for Class registration.");
       		String startTransaction = "SET TRANSACTION READ WRITE";
        	
        	String dropTableWarehouses = "drop table warehouses cascade constraints";
       		String createTableWarehouses = "create table warehouses (" + 
           		"id number(10)," + 
           		"name varchar2(20)," + 
        		"address varchar2(40)," + 
            	"city varchar2(15)," + 
        		"state varchar2(15)," +   
            	"zip number(5)," +  
            	"tax_rate number(6,2) check (tax_rate > 0)," +    
            	"sales_sum number(15) default 0," +  
           		"primary key(id) )";
            String dropTableDistributionStation = "drop table distribution_station cascade constraints";
            String createTableDistributionStation = "create table distribution_station (" + 
            	"warehouse_id number(10)," +
            	"id number(10)," +
            	"name varchar2(20)," +
            	"address varchar2(40)," +
            	"city varchar2(15)," +
            	"state varchar2(15)," +
            	"zip number(5)," +
            	"tax_rate number(6,2) check(tax_rate>0)," +
            	"sales_sum number(15)," +
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
            	"zip number(5)," +
            	"phone varchar2(15)," +
            	"initial_date date," +
            	"discount number(8)," +
            	"outstanding_balance number(10)," +
            	"year_spend number(10)," +
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
            System.out.println("connection established successfully.\n");
            //statement.executeUpdate(startTransaction);
            System.out.println("transaction started successfully\n");
            try {
            statement.executeUpdate(dropTableWarehouses);
            } 
            catch (Exception e) 
            {}
            statement.executeUpdate(createTableWarehouses);
            System.out.println("table warehouses created successfully\n");
            try {
            	statement.executeUpdate(dropTableDistributionStation);
            }
            catch (Exception e)
            {}
            statement.executeUpdate(createTableDistributionStation);
            System.out.println("table distribution_station created successfully\n");
            try {
            	statement.executeUpdate(dropTableCustomers);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableCustomers);
            System.out.println("table customers created successfully\n");
            try {
            	statement.executeUpdate(dropTableOrders);
            }
            catch (Exception e)
            {}
            statement.executeUpdate(createTableOrders);
            System.out.println("table orders created successfully\n");
            try {
            	statement.executeUpdate(dropTableLineItems);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableLineItems);
            System.out.println("table lineItems created successfully\n");
            try {
            	statement.executeUpdate(dropTableWarehouseStock);
            } 
            catch (Exception e)
            {}
            statement.executeUpdate(createTableWarehouseStock);
            System.out.println("table warehouse_stock created successfully\n");
            statement.executeUpdate("COMMIT");
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
		username = "xih49"; //This is your username in oracle
		password = "3974444"; //This is your password in oracle
	
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
