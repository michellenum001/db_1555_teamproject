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
            	"tax_rate number(6) check (tax_rate > 0)," +    
            	"sales_sum number(15) default 0," +  
           		"primary key(id) )";    
       
            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            statement.executeUpdate(dropTableWarehouses);
            statement.executeUpdate(createTableWarehouses);
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