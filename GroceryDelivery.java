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
    private ResultSet resultSet, resultSet2, resultSet3; //used to hold the result of your query (if one
    // exists)
    private String query;  //this will hold the query we are using
    
    public int selectChoice() {
        
        Scanner scan = new Scanner(System.in);
        
        System.out.println("---------------------------------------------------------");
        System.out.println("These are the possible operations: ");
        System.out.println("0: Initialize the Database");
        System.out.println("1: New Order Transaction");
        System.out.println("2: Payment Transaction");
        System.out.println("3: Order Status Transaction");
        System.out.println("4: Delivery Transaction");
        System.out.println("5: Stock Level Transaction");
        System.out.println("6: Exit");
        System.out.print("\nEnter the number corresponding to the desired operation: ");
        
        int choice = -1;
        
        while (choice < 0 || choice > 6) {
            try {
                choice = Integer.parseInt(scan.next());
                if (choice < 0 || choice >6) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            catch (Exception e) {
                System.out.print("Invalid input. Please try again: ");
            }
        }
        return choice;
    }
    
    public GroceryDelivery()
    {
        int choice = -1;
        System.out.println();
        //System.out.println("Initially set up the database...You can also set up the database manually.");
        //createTables();
        //populateTables();
        while (choice < 0 || choice != 6) {
            choice = selectChoice();
            
            if (choice == 0) {
                createTables();
                populateTables();
            }
            
            if (choice == 1) {
                newOrderTransaction();
            }
            
            if (choice == 2) {
                paymentTransaction();
            }
            
            if (choice == 3) {
                orderStatusTransaction();
            }
            
            if (choice == 4) {
                deliveryTransaction();
            }
            
            if (choice == 5) {
                stockLevelTransaction();
            }
            
        }
    }
    
    public void newOrderTransaction() {
        int distribution_station = -1;
        int warehouse_id = -1;
        int custID = -1;
        Scanner scan = new Scanner(System.in);
        try {
            System.out.println("\n The following info will be used to determine the unique customer: ");
            System.out.print("Enter the warehouse ID: ");
            while (warehouse_id < 1) {
                try{
                    warehouse_id = Integer.parseInt(scan.next());
                    if(warehouse_id < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e){
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            System.out.print("Enter the distribution station ID: ");
            while (distribution_station < 1) {
                try {
                    distribution_station = Integer.parseInt(scan.next());
                    if (distribution_station < 1){
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            System.out.print("Enter the customer ID: ");
            while (custID < 1){
                try{
                    custID = Integer.parseInt(scan.next());
                    if(custID < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e){
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            // Will have a quick transaction to get some necessary info for the order.
            // Then we commit and start a new transaction at the end when we are
            // ready to execute all our updates to the database.
            
            String startTransaction = "SET TRANSACTION READ WRITE";
            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            int maxItemID, orderID;
            resultSet = statement.executeQuery("select max(id) from items");
            if(resultSet.next()){
                maxItemID = resultSet.getInt(1);
                System.out.println("\n The database contains " + maxItemID + " unique items.");
            }
            else {
                System.out.println("\nThere are no items in the database.");
                return;
            }
            double[] itemPrices = new double[maxItemID + 1];
            resultSet = statement.executeQuery("select id, price from items");
            while(resultSet.next()){
                int id = resultSet.getInt(1);
                double price = resultSet.getDouble(2);
                itemPrices[id] = price;
            }
            String userLastOrder = "select max(id) from orders where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_station + " and custID = " + custID;
            resultSet = statement.executeQuery(userLastOrder);
            if (resultSet.next()){
                orderID = resultSet.getInt(1) + 1;
                System.out.println("Last order for this customer was #" + (orderID-1) + ", so we now have order #" + orderID + "\n");
            }
            else{
                System.out.println("This customer is not found in the database.");
                return;
            }
            
            // Got preliminary info that we needed - will close transaction now
            // while we get user input for the order info
            statement.executeUpdate("COMMIT");
            
            ArrayList <String> sqlStatements = new ArrayList <String> ();
            
            int numLineItems = 0;
            System.out.print("Please input the number of unique items (# of line items) for this order. Keep in mind that each item can only be used in one line item, so the maximum will be " + maxItemID + ": ");
            while(numLineItems <=0 ){
                //numLineItems = Integer.parseInt(scan.next());
                try{
                    numLineItems = Integer.parseInt(scan.next());
                    if(numLineItems < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                    if (numLineItems > maxItemID) {
                    	System.out.println("More than the # of items: Setting to " + maxItemID);
                    	numLineItems = maxItemID;
                    }
                }
                catch (Exception e){
                    System.out.print("Invalid input. Please try again: ");
                }
            }
           
            String insertOrder = "insert into orders (warehouse_id, distributor_id, custID, " +
            "id, order_date, completed, num_lineItems) values (" + warehouse_id + ", " +
            distribution_station + ", " + custID + ", " + orderID + ", " +
            "SYSDATE" + ", 0, " + numLineItems + ")";
            //System.out.println(insertOrder);
            //statement.executeUpdate(insertOrder);
            sqlStatements.add(insertOrder);
            ArrayList <Integer> usedItems = new ArrayList <Integer> ();
            for (int i=0; i<numLineItems; i++){
                System.out.println("Please input the basic information about No." + (i+1) + " item:");
                //System.out.print("Please input the item ID for this line item: ");
                int item_id = -1;
                while(true){
                    System.out.print("Please input the item ID for this line item: ");
                    item_id = Integer.parseInt(scan.next());
                    if(item_id < 1 || item_id > maxItemID){
                        System.out.print("Invalid input.");
                    }
                    else if (usedItems.contains(item_id)){
                        System.out.println("This item has been added into the order.");
                    }
                    else{
                        break;
                    }
                }
                usedItems.add(item_id);
                int quantity = -1;
                //System.out.print("Please input the quantity for this item: ");
                while(quantity <= 0){
                    System.out.print("Please input the quantity for this item: ");
                    quantity = Integer.parseInt(scan.next());
                    if(quantity <= 0){
                        System.out.print("Invalid input.");
                    }
                }
                double item_price = itemPrices[item_id]*quantity;
                String insertLineItem = "insert into LineItems (warehouse_id, distributor_id, " +
                "custID, order_id, id, item_id, quantity, price, date_delivered) values (" +
                warehouse_id + ", " + distribution_station + ", " + custID + ", " + orderID + ", " +
                (i+1) + ", " + item_id + ", " + quantity + ", " + item_price + ", NULL)";
               // statement.executeUpdate(insertLineItem);
               sqlStatements.add(insertLineItem);
            }
            
            // Now that we are done with user input and getting the order info, 
            // we can run all the statements.
            
            statement.executeUpdate(startTransaction);
            for (String insertStatement: sqlStatements) {
            	//System.out.println("trying: " + insertStatement);
            	statement.executeUpdate(insertStatement);
            }
            
            statement.executeUpdate("COMMIT");
            System.out.println("\nTransaction committed.\n");
            
        } catch(SQLException Ex) {
            System.out.println("Error running the sample queries.  Machine Error: " +
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
    
    public void stockLevelTransaction() {
    
        int distribution_station = -1;
        int warehouse_id = -1;
        int stock_threshold = -1;
        Scanner scan = new Scanner(System.in);
        
        try {
            System.out.println("\nThe following info will be used to determine the " +
                               "distribution station and integer stock threshold: ");
            
            System.out.print("Enter the warehouse ID: ");
            
            while (warehouse_id < 1) {
                try {
                    warehouse_id = Integer.parseInt(scan.next());
                    
                    if (warehouse_id < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the distribution station: ");
            
            
            while (distribution_station < 1) {
                try {
                    distribution_station = Integer.parseInt(scan.next());
                    
                    if (distribution_station < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the stock threshold: ");
            while (stock_threshold < 1) {
                try {
                    stock_threshold = Integer.parseInt(scan.next());
                    
                    if (stock_threshold < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            // Now that we have finished collecting user input, we can start the transaction
            
            String startTransaction = "SET TRANSACTION READ WRITE";
            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            
            //The 20 most recent orders for the distribution station
            String lastTwentyOrders = "Select * from (Select order_date, custID, id from orders where warehouse_id = " +
            warehouse_id + " and distributor_id = " + distribution_station +
            " order by order_date desc) sortedDate where rownum < 21";
            
            //This will initially hold all the items in the line items for the given orders.
            //Later it will be reduced to only the items that have a stock under the threshold.
            //I am using a TreeSet since it won't store duplicate itemIDs among the line items.
            
            TreeSet <Integer> items = new TreeSet <Integer> ();
            
            resultSet = statement.executeQuery(lastTwentyOrders);
            
            //Can only have one resultSet with a statement, so we make a second here to
            //avoid overwriting the resultSet of the findLineItems query.
            
            Statement statement2 = connection.createStatement();
            int orderCount = 1;
            
            while (resultSet.next()) {
                String date = resultSet.getDate(1).toString();
                int custID = resultSet.getInt(2);
                int orderID = resultSet.getInt(3);
                
                String lineItems = "Select item_id, quantity from lineItems where " +
                "warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_station +
                " and custID = " + custID + " and order_id = " + orderID;
                resultSet2 = statement2.executeQuery(lineItems);
                int lineItemCount = 1;
                while (resultSet2.next()) {
                    int itemID = resultSet2.getInt(1);
                    int quantity = resultSet2.getInt(2);
                    items.add(itemID);
                    /*
                     System.out.println("\nLine item #" + lineItemCount + " of order #" + orderCount + " by date, date of " + date + ":\n----------------------------------");
                     System.out.println("itemID: " + itemID);
                     System.out.println("Quantity: " + quantity);
                     */
                    lineItemCount++;
                }
                orderCount++;
            }
            statement2.close();
            System.out.println();
            
            int cntResult = 0;
            
            String warehouseStock = "Select item_id, quantity_in_stock from warehouse_stock where " +
            "warehouse_id = " + warehouse_id + " and quantity_in_stock < " + stock_threshold;
            resultSet = statement.executeQuery(warehouseStock);
            
            ArrayList <Integer> itemsQualifying = new ArrayList <Integer> ();
            
            while (resultSet.next()) {
                
                int itemID = resultSet.getInt(1);
                int quantity = resultSet.getInt(2);
                /*
                 System.out.println("\nItemID: " + itemID);
                 System.out.println("Quantity: " + quantity);
                 System.out.println("Below threshold: " + (quantity < stock_threshold));
                 */
                
                //See if the item was in a relevant line item order.
                if (items.contains(itemID)) {
                    cntResult++;
                    items.remove(itemID);
                    itemsQualifying.add(itemID);
                }
            }
            
            System.out.println("There are a total of " + cntResult + " items that have a "
                               + "stock under the threshold in the distribution station's warehouse.");
            System.out.println("They are items: " + itemsQualifying);
            
            statement.executeUpdate("COMMIT");
            System.out.println("\nTransaction committed.\n");
            
        } catch(SQLException Ex) {
            System.out.println("Error running the sample queries.  Machine Error: " +
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
    
    public void deliveryTransaction() {
        int warehouse_id = -1;
        int distribution_id = -1;
        Scanner scan = new Scanner(System.in);
        try {
            System.out.println("\n The following info will be used to determine the distribution station: ");
            System.out.print("Enter the warehouse ID: ");
            while (warehouse_id < 1) {
                try {
                    warehouse_id = Integer.parseInt(scan.next());
                    if (warehouse_id < 1){
                        System.out.println("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.println("Invalid input. Please try again: ");
                }
            }
            
            // Now that we are done collecting user input, we can start the transaction
            
            String startTransaction = "SET TRANSACTION READ WRITE";
            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            System.out.println("Transaction started successfully.");
            
            //Set up discount array for customers.
            String totalCustomers = "Select count(*) from customers";
            resultSet = statement.executeQuery(totalCustomers);
            resultSet.next();
            double [] discounts = new double [resultSet.getInt(1) + 1];
            
            //Fill in discount array for customers
            String findDiscounts = "Select id, discount from customers";
            resultSet = statement.executeQuery(findDiscounts);
            while (resultSet.next()) {
            	discounts[resultSet.getInt(1)] = Math.round(resultSet.getDouble(2)*100.0)/100.0;
            }
            
            Statement statement2 = connection.createStatement();
            Statement statement3 = connection.createStatement();
            
            //Obtain the order info for incomplete orders
            String findOrders = "select warehouse_id, distributor_id, custID, id from orders where completed = 0";
            resultSet = statement.executeQuery(findOrders);
            while (resultSet.next()) {
            
            	int warehouseID = resultSet.getInt(1);
            	int distributorID = resultSet.getInt(2);
            	int custID = resultSet.getInt(3);
            	int orderID = resultSet.getInt(4);
            	
            	System.out.println("\nProcessing the following order: \n-----------------");
            	System.out.println("Warehouse ID = " + warehouseID);
            	System.out.println("Distributor ID = " + distributorID);
            	System.out.println("Customer ID = " + custID);
            	System.out.println("Order ID = " + orderID);
            	System.out.println("Customer Discount = " + discounts[custID] + "%");
            	
            	// If all items are in stock, then the order can be completed.
            	// Else, we will deliver the items in stock and keep the order marked incomplete.
            	boolean orderCompleted = true;
            	
            	//Obtain the line items for this order that have not been delivered yet
            	String lineItemsOfOrder = "select id, item_id, quantity, price from lineItems where warehouse_id = " + 
            	warehouseID + " and distributor_id = " + distributorID + " and custID = " + custID + 
            	" and order_id = " + orderID + " and date_delivered is NULL";
            	//System.out.println(lineItemsOfOrder);
            	
            	resultSet2 = statement2.executeQuery(lineItemsOfOrder);
            	while (resultSet2.next()) {
            		int lineItemID = resultSet2.getInt(1);
            		int itemID = resultSet2.getInt(2);
            		int quan = resultSet2.getInt(3);
            		double price = Math.round(resultSet2.getDouble(4)*100.0)/100.0;
            		
            		System.out.println("\nLine Item Information: \n----------------------");
            		System.out.println("Warehouse ID = " + warehouseID);
            		System.out.println("Distributor ID = " + distributorID);
            		System.out.println("Customer ID = " + custID);
            		System.out.println("Order ID = " + orderID);
            		System.out.println("Line Item ID = " + lineItemID);
            		System.out.println("Item ID = " + itemID);
            		System.out.println("Quantity = " + quan);
            		System.out.println("Total Price for " + quan + " items = " + price);
            		System.out.println("Customer Discount = " + discounts[custID] + "%");
            		System.out.println("Total w/ Discount = " + Math.round(100.0*((1.0-discounts[custID]/100.0)*price))/100.0 + "\n");
            		
            		double lineItemTotal = Math.round(100.0*(1.0-discounts[custID]/100.0)*price)/100.0;
            		
            		 //Make sure it's in stock.
                    String checkInStock = "select quantity_in_stock from warehouse_stock where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                    resultSet3 = statement3.executeQuery(checkInStock);
					resultSet3.next();
                    int quantityInStock = resultSet3.getInt(1);
        
        			//not in stock
                    if (quantityInStock < quan) {
                    	System.out.println("Item #" + itemID + " is currently out of stock for this quantity in warehouse #" + warehouseID + ", so it cannot be delivered right now.\n");
                   		orderCompleted = false;
                    }
                    //in stock. Now will deliver the line item and charge accordingly.
                    else {
                    
                    	//Now charging for the item, so increasing the outstanding balance
                    	String updateBalance = "update customers set outstanding_balance = outstanding_balance + " + lineItemTotal + " where warehouse_id = " + warehouseID + " and distributor_id = " + distributorID + " and id = " + custID;
                    	statement3.executeUpdate(updateBalance);
                    	System.out.println("Increased the outstanding balance by " + lineItemTotal);
                    	
                    	//Increasing the number of deliveries by the # of items delivered in this line item.
                    	String updateNumDeliveries = "update customers set num_deliveries = num_deliveries + " + quan + " where warehouse_id = " + warehouseID + " and distributor_id = " + distributorID + " and id = " + custID;
                    	statement3.executeUpdate(updateNumDeliveries);
                    	System.out.println("Increased the number of deliveries by " + quan + ".");
                    	
                    	String updateStockQuantity = "update warehouse_stock set quantity_in_stock = quantity_in_stock - " + quan + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                    	statement3.executeUpdate(updateStockQuantity);
                    	System.out.println("Decreased stock of this item in warehouse by " + quan);
                    	
                    	String updateStockSold = "update warehouse_stock set quantity_sold = quantity_sold + " + quan + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                    	statement3.executeUpdate(updateStockSold);
                   	 	System.out.println("Increased quantity of sold for this item in warehouse by " + quan);
                   	 	
                   	 	String updateStockOrder = "update warehouse_stock set number_orders = number_orders + 1" + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                    	statement3.executeUpdate(updateStockOrder);
                    	System.out.println("Increased number of orders for this item in warehouse by 1.");	
                    	
                    	String setDelivered = "update LineItems set date_delivered = " + "SYSDATE" + 
                    	" where warehouse_id = " + warehouse_id + " and distributor_id = " + distributorID +
                    	" and custID = " + custID + " and order_id = " + orderID + " and id = " + lineItemID;
                    	statement3.executeUpdate(setDelivered);
                    	System.out.println("Set the delivery date of this line item to the current date.\n");
            
                    	
                    }                  
            	} // done looping through all line items of a given order
            	 
                // if all the pending line items were in stock, then we have delivered
            	// all and can mark the order as completed.
                if (orderCompleted == true) {
                    String orderComplete = "update orders set completed = 1 where warehouse_id = " +
                    warehouseID + " and distributor_id = " + distributorID + " and custID = " +
                    custID + " and id = " + orderID;
                    statement3.executeUpdate(orderComplete);
                    System.out.println("Marked the order as complete.\n");
                 }
                 else {
                 	System.out.println("This order is still incomplete, as certain items are out of stock.\n");
                 }
            }
            statement3.close();
            statement2.close();
             
            statement.executeUpdate("COMMIT");
            System.out.println("Transaction committed.\n");
            
            /*
            String findDistribution  = "select id from distribution_station where warehouse_id = " + warehouse_id;
            resultSet = statement.executeQuery(findDistribution);
            while (resultSet.next()) {
                distribution_id = resultSet.getInt(1);
                String findLineItems = "select custID, order_id, item_id, quantity, price from lineItems where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id + " and date_delivered is NULL";
                Statement statement2 = connection.createStatement();
                Statement statement3 = connection.createStatement();
                resultSet2 = statement2.executeQuery(findLineItems);
                int cntRows = 0;
                while(resultSet2.next()){
                    cntRows++;
                    int custID = resultSet2.getInt(1);
                    int orderID  = resultSet2.getInt(2);
                    int item_id = resultSet2.getInt(3);
                    int quan = resultSet2.getInt(4);
                    double price = resultSet2.getInt(5);
                    System.out.println("\nRecord found for this order line items: " + custID + ", " + orderID + ", " + price);
                    System.out.println("warehouse id: " + warehouse_id);
                    System.out.println("distribution station id: " + distribution_id);
                    System.out.println("customer id: " + custID);
                    System.out.println("order id: " + orderID);
                    System.out.println("The quantity for this line item is: " + quan);
                    System.out.println("price is: " + price);
                    
                    //Make sure it's in stock.
                    String checkInStock = "select quantity_in_stock from warehouse_stock where warehouse_id = " + warehouse_id + " and item_id = " + item_id;
                    resultSet3 = statement3.executeQuery(checkInStock);
					resultSet3.next();
                    int quantityInStock = resultSet3.getInt(1);
                    
                    if (quantityInStock < quan) {
                    	System.out.println("Item #" + item_id + " is currently out of stock in warehouse #" + warehouse_id + ", so it cannot be delivered right now.");
                    }
                    else {
                    
                    	//The order is completed.
                    	String updateCompleted = "update orders set completed = 1" + " where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id + " and custID = " + custID + " and id = " + orderID;
                    	statement3.executeUpdate(updateCompleted);
                    	System.out.println("Marked this line item as delivered.");
                    	
                    	//Now charging for the item, so increasing the outstanding balance
                    	String updateBalance = "update customers set outstanding_balance = outstanding_balance + " + price + " where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id + " and id = " + custID;
                    	statement3.executeUpdate(updateBalance);
                    	System.out.println("Increased the outstanding balance by " + price);
                    	
                    	//
                    	String updateNumDeliveries = "update customers set num_deliveries = num_deliveries + " + quan + " where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id + " and id = " + custID;
                    	statement3.executeUpdate(updateNumDeliveries);
                    	System.out.println("Increased the number of deliveries by " + quan + ".");
                    	
                    	
                    	//Commented out because customers only get charged at this step.
                    	//String updateNumberPayments = "update customers set number_payments = number_payments + 1" + " where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id + " and id = " + custID;
                    	//statement3.executeUpdate(updateNumberPayments);
                    	//System.out.println("Increased the number of payments by 1.");
                    	
                    	
                    	String updateStockQuantity = "update warehouse_stock set quantity_in_stock = quantity_in_stock - " + quan + " where warehouse_id = " + warehouse_id + " and item_id = " + item_id;
                    	statement3.executeUpdate(updateStockQuantity);
                    	System.out.println("Decrease stock of this item in warehouse by " + quan);
                    	
                    	String updateStockSold = "update warehouse_stock set quantity_sold = quantity_sold + " + quan + " where warehouse_id = " + warehouse_id + " and item_id = " + item_id;
                    	statement3.executeUpdate(updateStockSold);
                   	 	System.out.println("Increase quantity of sold for this item in warehouse by " + quan);
                   	 	
                   	 	String updateStockOrder = "update warehouse_stock set number_orders = number_orders + 1" + " where warehouse_id = " + warehouse_id + " and item_id = " + item_id;
                    	statement3.executeUpdate(updateStockOrder);
                    	System.out.println("Increase number of orders for this item in warehouse by 1.");
                    }
                }
                statement3.close();
                if(cntRows > 0){
                    //String date = "TO_DATE('10-10-10', 'MM-DD-YYYY')";
                    String setDelivered = "update LineItems set date_delivered = " + "SYSDATE" + " where warehouse_id = " + warehouse_id + " and distributor_id = " + distribution_id;
                    //System.out.println(setDelivered);
                    statement2.executeQuery(setDelivered);
                    System.out.println("\n Set the deliveries to not NULL.");
                }
                else{
                    System.out.println("\n All order line items for the given distribution station are already delivered.");
                }
                statement2.close();
            }
            */
            
           
        } catch(SQLException Ex){
            System.out.println("Error running the sample queries. Machine Error: " + Ex.toString());
        } finally{
            try{
                if(statement != null){
                    statement.close();
                }
                if(preparedStatement != null){
                    preparedStatement.close();
                }
            } catch (SQLException e){
                System.out.println("Cannot close Statement. Machine error: " + e.toString());
            }
        }
    }
    
    public void orderStatusTransaction() {
        
        int distribution_station = -1;
        int custID = -1;
        int warehouse_id = -1;
        int order_id = -1;
        Scanner scan = new Scanner(System.in);
        
        try {
            System.out.println("\nThe following info will be used to determine the unique customer: ");
            
            System.out.print("Enter the warehouse ID: ");
            
            while (warehouse_id < 1) {
                try {
                    warehouse_id = Integer.parseInt(scan.next());
                    
                    if (warehouse_id < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the distribution station: ");
            
            
            while (distribution_station < 1) {
                try {
                    distribution_station = Integer.parseInt(scan.next());
                    
                    if (distribution_station < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the customer ID for the distribution station: ");
            
            while (custID < 1) {
                try {
                    custID = Integer.parseInt(scan.next());
                    
                    if (custID < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            //Now that we done collecting user input, we can start the transaction.
            String startTransaction = "SET TRANSACTION READ WRITE";
            
            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            System.out.println("Transaction started successfully.\n");
            
            String checkOrder = "Select id from orders where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and custID = " + custID +
            " and order_date = (select max(order_date) from orders where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and custID = " + custID + ") and rownum = 1";
            
            resultSet = statement.executeQuery(checkOrder);
            int orderID = -1;
            
            if (resultSet.next()) {
                orderID = resultSet.getInt(1);
                System.out.println("Using orderID = " + orderID);
            }
            else {
                System.out.println("No record for this customer.");
                return;
            }
            
            String lineInfo = "Select item_id, quantity, price, date_delivered from lineItems " +
            "where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and custID = " + custID +
            " and order_id = " + orderID;
            resultSet = statement.executeQuery(lineInfo);
            
           	int cnt = 1;
            while (resultSet.next()) {
                System.out.println("Line Item #" + cnt + "\n-----------------------------");
                System.out.println("Item_id: " + resultSet.getInt(1));
                System.out.println("Quantity of item: " + resultSet.getInt(2));
                System.out.println("Amount due: " + resultSet.getDouble(3));
                if (resultSet.getDate(4) != null) {
                    System.out.println("Date delivered: " + resultSet.getDate(4).toString());
                }
                else {
                    System.out.println("Not delivered yet.");
                }
                System.out.println();
                cnt++;
            }
            
            statement.executeUpdate("COMMIT");
            System.out.println("Transaction committed.\n");
            
        } catch(SQLException Ex) {
            System.out.println("Error running the sample queries.  Machine Error: " +
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
    
    public void paymentTransaction() {
        
        int distribution_station = -1;
        int custID = -1;
        int warehouse_id = -1;
        Scanner scan = new Scanner(System.in);
        
        try {
            System.out.println("\nThe following info will be used to determine the unique customer: ");
            
            System.out.print("Enter the warehouse ID: ");
            
            while (warehouse_id < 1) {
                try {
                    warehouse_id = Integer.parseInt(scan.next());
                    
                    if (warehouse_id < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the distribution station: ");
            
            
            while (distribution_station < 1) {
                try {
                    distribution_station = Integer.parseInt(scan.next());
                    
                    if (distribution_station < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            System.out.print("Enter the customer ID for the distribution station: ");
            
            while (custID < 1) {
                try {
                    custID = Integer.parseInt(scan.next());
                    
                    if (custID < 1) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
           
            
            double paymentAmt = -1;
            
            System.out.print("Enter the amount of the payment: ");
            
            while (paymentAmt < 0) {
                try {
                    paymentAmt = Double.parseDouble(scan.next());
                    
                    if (paymentAmt < 0) {
                        System.out.print("Invalid input. Please try again: ");
                    }
                }
                catch (Exception e) {
                    System.out.print("Invalid input. Please try again: ");
                }
            }
            
            //Can start transaction now that we have finished taking user input.
            String startTransaction = "SET TRANSACTION READ WRITE";

            statement = connection.createStatement();
            statement.executeUpdate(startTransaction);
            System.out.println("\nTransaction started successfully.\n");
            
            String checkCustomer = "Select outstanding_balance from customers where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and id = " + custID;
            
            resultSet = statement.executeQuery(checkCustomer);
            double outstanding_balance = 0;
            
            if (resultSet.next()) {
                outstanding_balance = resultSet.getDouble(1);
                System.out.println("Record found for this customer: Outstanding balance = " + outstanding_balance + " prior to payment.");
               
               // If outstanding balance is already 0, don't need to pay towards it.
               if (outstanding_balance == 0) {
                	System.out.println("Outstanding balance is 0, thus no payments need be applied. Ending transaction and returning now.");
                	statement.executeUpdate("COMMIT");
                	return;
                }
                
                // We will not allow a negative outstanding balance (credit).
                // So we only pay to the point where we can get the outstanding balance to zero.
                if (paymentAmt > outstanding_balance) {
                
                	System.out.println("Outstanding balance is only " + outstanding_balance + 
                	", while payment amount is " + paymentAmt + ". Thus, will only apply " + 
                	outstanding_balance + " to the outstanding balance to bring it to zero.");
                	
                	paymentAmt = outstanding_balance;
                }
            }
            else {
                System.out.println("No record for this customer. Ending transaction and returning.");
                statement.executeUpdate("COMMIT");
                return;
            }
            
           
            //Can start transaction now that we have finished taking user input.
            
            ArrayList <String> sqlStatements = new ArrayList <String> ();
            
            String updateCustomer = "Update customers set outstanding_balance = " +
            (outstanding_balance - paymentAmt) + " where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and id = " + custID;
            
            statement.executeQuery(updateCustomer);
       
            System.out.println("\nPaid " + paymentAmt + " towards the outstanding balance in the customers table.");
            
            String updateYearSpent = "update customers set year_spend = year_spend + " + paymentAmt + " where warehouse_id = 1 and " + "distributor_id = " + distribution_station + " and id = " + custID;
            statement.executeQuery(updateYearSpent);
        
            System.out.println("Added " + paymentAmt + " towards the year spend amount in the customers table.");
            
            String updateDistribution = "Update distribution_station set sales_sum = sales_sum + " +
            paymentAmt + "where warehouse_id = 1 and " +
            "id = " + distribution_station;
            
            statement.executeQuery(updateDistribution);
            
            System.out.println("Added " + paymentAmt + " towards the sales sum of distribution center #" + distribution_station + ".");
            
            String updateWarehouse = "Update warehouses set sales_sum = sales_sum + " +
            paymentAmt + "where id = 1";
            
            statement.executeQuery(updateWarehouse);
          
            System.out.println("Added " + paymentAmt + " towards the sales sum of warehouse #1.\n");           
            
            statement.executeUpdate("COMMIT");
            System.out.println("Transaction committed.\n");
            
        } catch(SQLException Ex) {
            System.out.println("Error running the sample queries.  Machine Error: " +
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
    
    public void populateTables(){
        try
        {
            Scanner scan = new Scanner(System.in);
            int numberWarehouses = 1;
            int numberDistribution = 3;
            int numberCustomers = 10;
            int numberItems = 100;
            int maxOrdersPerCustomer = 5;
            int minLineItemsPerOrder = 3;
            int maxLineItemsPerOrder = 7;
            
            System.out.println("The default number for warehouses is 1: ");
            System.out.print("Please input the number of distribution stations (the default number is 3): ");
            numberDistribution = Integer.parseInt(scan.next());
            if(numberDistribution < 1){
                System.out.println("Invalid input. The number of distribution stations is set as 3.");
                numberDistribution = 3;
            }
            System.out.print("Please input the number of customers for each distribution station (the default number is 10): ");
            numberCustomers = Integer.parseInt(scan.next());
            if(numberCustomers < 1){
                System.out.println("Invalid input. The number of customers for each distributon station is set as 10.");
                numberCustomers = 10;
            }
            
            System.out.print("Please input the number of items (the default value is 100): ");
            numberItems = Integer.parseInt(scan.next());
            if(numberItems < 1){
                System.out.println("Invalid input. The number of items is set as 100.");
                numberItems = 100;
            }
            System.out.print("Please input the max number of orders for each customers (the default value is 5): ");
            maxOrdersPerCustomer = Integer.parseInt(scan.next());
            if(maxOrdersPerCustomer < 1){
                System.out.println("Invalid input. The max number of orders is set as 5.");
                maxOrdersPerCustomer = 5;
            }
            System.out.print("Please input the min number of line items (the default value is 3): ");
            minLineItemsPerOrder = Integer.parseInt(scan.next());
            if(minLineItemsPerOrder < 1){
                System.out.println("Invalid input. The min number of line items is set as 3.");
                minLineItemsPerOrder = 3;
            }
            if (minLineItemsPerOrder > numberItems) {
            	System.out.println("There are only " + numberItems + " items. Setting to " + numberItems + ".");
            	minLineItemsPerOrder = numberItems;
            }
            System.out.print("Please input the max number of line items (the default value is 7): ");
            maxLineItemsPerOrder = Integer.parseInt(scan.next());
            if(maxLineItemsPerOrder < 1){
                System.out.println("Invalid input. The max number of line items is set as 7.");
                maxLineItemsPerOrder = 7;
            }
            
            if (maxLineItemsPerOrder > numberItems) {
            	System.out.println("There are only " + numberItems + " items. Setting to " + numberItems + ".");
            	maxLineItemsPerOrder = numberItems;
            }
            if (maxLineItemsPerOrder < minLineItemsPerOrder) {
            	System.out.println("The maximum number of line items per order cannot be lower than the minimum number. Setting to " + numberItems + ".");
            	maxLineItemsPerOrder = numberItems;
            }
           
            
            double warehouseSale  = 0;
            ArrayList<Double> distributorSale = new ArrayList<Double>();
            for (int i=0;i<numberDistribution;i++){
                distributorSale.add(0.0);
            }
            
            String startTransaction = "SET TRANSACTION READ WRITE";
            String insertWarehouses = "insert into warehouses values(1, 'GrowRoom', '1008 Ross Park Mall Dr', 'Pittsburgh', 'PA', '15237', 0.07, 0)";
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
                //i%1000 to avoid too long entries and getting an error
                
                int warehouse_number = 1;
                int id = i + 1;
                String name = "name" + i%1000;
                String address = "address" + i%1000;
                String city = "city" + i%1000;
                String state = "state" + i%1000;
                String zip = (15000 + i%1000) + "";
                double tax_rate = r.nextDouble()*40 + 1;
                double sales_sum = 0;
                
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
                statement.executeUpdate(insertDistribution);
                //System.out.println(insertDistribution);
                
            }
            System.out.println("Inserted " + numberDistribution + " tuples into the distribution_station table.");
            
            ArrayList <Double> itemPrice = new ArrayList <Double> ();
            //insert into items
            for (int i = 0; i < numberItems; i++)
            {
                int id = i + 1;
                String name = "item" + i;
                double price = Math.round(r.nextDouble()*100+1) + Math.round(r.nextDouble()*100)/100.0;
               // System.out.println(price);
                
                String insertItem = "insert into items values(" +
                id + ", " +
                "'" + name + "', " +
                price + ")";
                itemPrice.add(price);
                statement.executeUpdate(insertItem);
                //System.out.println(insertItem);
                
            }
            System.out.println("Inserted " + numberItems + " tuples into the items table.");
            
            // insert into customers
            for (int i=0; i< numberDistribution; i++){
                for (int j=0; j<numberCustomers; j++){
                    String fname = "fname" + j;
                    String middle_init = "a" + j;
                    String lname = "lname" + j;
                    String address = "address" + j;
                    String city = "city" + j;
                    String state = "state" + j;
                    String zip = (15000 + i%1000) + "";
                    String phone = "phone" + j;
                    int month = r.nextInt(12) + 1;
                    int day = r.nextInt(28) + 1;
                    int year = r.nextInt(6) + 2010;
                    String initial_date = "TO_DATE('" + month + "-" + day + "-" + year + "', 'MM-DD-YYYY')";
                    double discount = r.nextDouble()*10;
                    double outstanding_balance = r.nextDouble()*1000;
                    //double year_spend = r.nextDouble()*1000 + outstanding_balance;
                    double year_spend = 0;
                    //int number_payments = r.nextInt(20);
                    int number_payments = 0;
                    int number_deliveries = 0;
                    String insertCustomer = "insert into customers values(1" + ", " + (i+1) + ", " + (j+1) + ", " + "'" + fname + "'" + ", " + "'" + middle_init + "'" + ", " + "'" + lname + "'" + ", " + "'" + address + "'" + ", " + "'" + city + "'" + ", " + "'" + state + "'" + ", " + "'" + zip + "'" + ", " + "'" + phone + "'" + ", " + initial_date + ", " + Math.round(discount*100)/100 + ", " + Math.round(outstanding_balance*100)/100 + "," + year_spend + ", " + number_payments + ", " + number_deliveries + ")";
                    try{
                        statement.executeUpdate(insertCustomer);
                        //System.out.println(insertCustomer);
                    }
                    catch (Exception e){
                        System.out.println(insertCustomer);
                        statement.executeUpdate(insertCustomer);
                        System.exit(0);
                    }
                }
            }
            System.out.println("Inserted " + (numberCustomers*numberDistribution) + " tuples into the customers table.");
            
            int [] quantityOrderedPerItem = new int [numberItems + 1];
            
            //Number is dependent on random outcome, so will keep track.
            int orderNumInserted = 0;
            int lineItemNumInserted = 0;
            //insert into orders, lineItems
            for (int i=0; i<numberDistribution; i++){
                for (int j = 0; j< numberCustomers; j++){
                    int numOrders = r.nextInt(maxOrdersPerCustomer) + 1;
                    orderNumInserted += numOrders;
                    for(int k = 0; k<numOrders; k++){
                        ArrayList <Integer> entered = new ArrayList <Integer> ();
                        int month = r.nextInt(12) + 1;
                        int day = r.nextInt(28) + 1;
                        int year = r.nextInt(6) + 2010;
                        String order_date = "TO_DATE('" + month + "-" + day + "-" + year + "', 'MM-DD-YYYY')";
                        int completed = 0;
                        int num_lineItems = r.nextInt(maxLineItemsPerOrder-minLineItemsPerOrder+1)+minLineItemsPerOrder;
                        String insertOrder = "insert into orders values(1" + ", " + (i+1) + ", " + (j+1) + ", " + (k+1) + ", " + order_date + ", " + completed + ", " + num_lineItems + ")";
                        statement.executeUpdate(insertOrder);
                        //System.out.println(insertOrder);
                        for(int m = 0; m<num_lineItems; m++){
                            int item_id = r.nextInt(numberItems) +1;
                            while(entered.contains(item_id)){
                                item_id = r.nextInt(numberItems) +1;
                            }
                            entered.add(item_id);
                            int quantity = r.nextInt(10) + 1;
                            quantityOrderedPerItem[item_id] += quantity;
                            double price = itemPrice.get(item_id-1)*quantity;
                            int monthDeliv = r.nextInt(12) + 1;
                            int dayDeliv = r.nextInt(28) + 1;
                            int yearDeliv = r.nextInt(6) + 2010;
                            //String date_delivered = "TO_DATE('" + monthDeliv + "-" + dayDeliv + "-" + yearDeliv + "', 'MM-DD-YYYY')";
                            String date_delivered = "NULL";
                            String insertLineItem = "insert into LineItems values(1," + (i+1) + ", " + (j+1) + ", " + (k+1) + ", " + (m+1) + ", " + item_id + ", " + quantity + ", " + price + ", " + date_delivered + ")";
                            lineItemNumInserted++;
                            statement.executeUpdate(insertLineItem);
                            //System.out.println(insertLineItem);
                        }
                    }
                }
            }
            System.out.println("Inserted " + orderNumInserted + " tuples into the orders table.");
            System.out.println("Inserted " + lineItemNumInserted + " tuples into the lineItems table.");
            
            for (int i = 0; i < numberItems; i++)
            {
                int warehouse_id = 1;
                int item_id = i + 1;
                
                // So that we don't run out of stock, we set in stock to what is ordered 
                // and then some random amount extra
                int inStock = quantityOrderedPerItem[item_id] + r.nextInt(200);
                
                int sold = 0;
                int orders = 0;
                
                String insertWarehouseStock = "insert into warehouse_stock values(" +
                warehouse_id + ", " +
                item_id + ", " +
                inStock + ", " +
                sold + ", " +
                orders + ")";
                
                //System.out.println(insertWarehouseStock);
                statement.executeUpdate(insertWarehouseStock);
                //System.out.println(insertWarehouseStock);
            }
            System.out.println("Inserted " + numberItems + " tuples into the warehouse_stock table.");
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
            "discount number(6,2)," +
            "outstanding_balance number(10,2)," +
            "year_spend number(10,2) default 0," +
            "number_payments number(10) default 0 ," +
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
        Scanner scan = new Scanner(System.in);
        System.out.print("\nEnter your Oracle username: ");
        
        username = scan.next(); //This is your username in oracle
        System.out.print("Enter your Oracle password: ");
        password = scan.next(); //This is your password in oracle
        
        
        
        try{
            // Register the oracle driver.
            DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
            
            //This is the location of the database.  This is the database in oracle
            //provided to the class
            String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
            
            //create a connection to DB on class3.cs.pitt.edu
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
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
