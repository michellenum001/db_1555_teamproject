//CS 1555 Term Project
//Fall 2015
//David Neiman, Xinyue Huang, Shijia Liu

import java.sql.*;  //import the file containing definitions for the parts
import java.text.ParseException;
//needed by java for database connection and manipulation
import java.sql.Date;
import java.util.*;

public class jdbcTransactionThread extends Thread {
    private static Connection connection; //used to hold the jdbc connection to the DB
    //private Statement statement; //used to create an instance of the connection
    private PreparedStatement preparedStatement; //used to create a prepared statement, that will be later reused
    private ResultSet resultSet, resultSet2, resultSet3; //used to hold the result of your query (if one
    // exists)
    private static GroceryDelivery grocery;
    
    private static int NUM_OF_THREADS = 15;
    private int choice;
    private int m_id;
    static int c_nextId = 1;
    
    synchronized static int getNextId(){
        return c_nextId++;
    }
    
    public jdbcTransactionThread(int c ) {
        super();
        choice = c;
        m_id = getNextId();
    }
    
    public void run(){
        System.out.println("thread " + m_id + " is executing transaction "+ choice);
        switch(choice){
            case 0:
                ExecuteNewOrderTransaction();
                //ExecutePaymentTransaction();
                //ExecuteOrderStatusTransaction();
                //ExecuteDeliveryTransaction();
                //ExecuteStockLevelTransaction();
                break;
            case 1:
                //ExecuteNewOrderTransaction();
                ExecutePaymentTransaction();
                //ExecuteOrderStatusTransaction();
                //ExecuteDeliveryTransaction();
                //ExecuteStockLevelTransaction();
                break;
            case 2:
                //ExecuteNewOrderTransaction();
                //ExecutePaymentTransaction();
                ExecuteOrderStatusTransaction();
                //ExecuteDeliveryTransaction();
                //ExecuteStockLevelTransaction();
                break;
            case 3:
                //ExecuteNewOrderTransaction();
                //ExecutePaymentTransaction();
                //ExecuteOrderStatusTransaction();
                ExecuteDeliveryTransaction();
                //ExecuteStockLevelTransaction();
                break;
            case 4:
                //ExecuteNewOrderTransaction();
                //ExecutePaymentTransaction();
                //ExecuteOrderStatusTransaction();
                //ExecuteDeliveryTransaction();
                ExecuteStockLevelTransaction();
                break;
            default:
                break;
        }
    }
    
    synchronized void ExecuteNewOrderTransaction() {
        Random r = new Random();
        int warehouse_id = 1;
        int distribution_station = r.nextInt(grocery.getNumDistribution()) + 1;
        int custID = r.nextInt(grocery.getNumCustomer()) + 1;
        int numLineItems = r.nextInt(grocery.getNumMaxLineItem()-grocery.getNumMinLineItem()+1) + grocery.getNumMinLineItem();
        newOrderTransaction(warehouse_id, distribution_station, custID, numLineItems);
        return;
    }
    
    synchronized void newOrderTransaction(int warehouse_id, int distribution_station, int custID, int numLineItems) {
        Statement statement = null;
        Random r = new Random();
        //int distribution_station = -1;
        //int warehouse_id = -1;
        //int custID = -1;
        //Scanner scan = new Scanner(System.in);
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            statement = connection.createStatement();
            System.out.println("\nTransaction 1 " + "for thread " + m_id +" started successfully.\n");
            // Will have a quick transaction to get some necessary info for the order.
            // Then we commit and start a new transaction at the end when we are
            // ready to execute all our updates to the database.
            
            //String startTransaction = "SET TRANSACTION READ WRITE";
            //System.out.println("yes****************");
            statement = connection.createStatement();
            //statement.executeUpdate(startTransaction);
            int maxItemID, orderID;
            resultSet = statement.executeQuery("select max(id) from items");
            if(resultSet.next()){
                maxItemID = resultSet.getInt(1);
                //System.out.println("\n The database contains " + maxItemID + " unique items.");
            }
            else {
                //System.out.println("\nThere are no items in the database.");
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
                //System.out.println("Last order for this customer was #" + (orderID-1) + ", so we now have order #" + orderID + "\n");
            }
            else{
                //System.out.println("This customer is not found in the database.");
                return;
            }
            
            // Got preliminary info that we needed - will close transaction now
            // while we get user input for the order info
            //statement.executeUpdate("COMMIT");
            
            ArrayList <String> sqlStatements = new ArrayList <String> ();
            
            String insertOrder = "insert into orders (warehouse_id, distributor_id, custID, " +
            "id, order_date, completed, num_lineItems) values (" + warehouse_id + ", " +
            distribution_station + ", " + custID + ", " + orderID + ", " +
            "SYSDATE" + ", 0, " + numLineItems + ")";
            //System.out.println(insertOrder);
            //statement.executeUpdate(insertOrder);
            sqlStatements.add(insertOrder);
            ArrayList <Integer> usedItems = new ArrayList <Integer> ();
            for (int i=0; i<numLineItems; i++){
                //System.out.println("The basic information about No." + (i+1) + " item is as follows:");
                //System.out.print("Please input the item ID for this line item: ");
                int item_id = 1;
                while(true){
                    item_id = r.nextInt(maxItemID)+1;
                    if (!usedItems.contains(item_id)){
                        break;
                    }
                }
                usedItems.add(item_id);
                int quantity = 1;
                //System.out.print("Please input the quantity for this item: ");
                quantity = r.nextInt(10)+1;
                //System.out.println("The item id is " + item_id + " and the quantity is " + quantity);
                double item_price = itemPrices[item_id]*quantity;
                String insertLineItem = "insert into LineItems (warehouse_id, distributor_id, " +
                "custID, order_id, id, item_id, quantity, price, date_delivered) values (" +
                warehouse_id + ", " + distribution_station + ", " + custID + ", " + orderID + ", " +
                (i+1) + ", " + item_id + ", " + quantity + ", " + item_price + ", NULL)";
                // statement.executeUpdate(insertLineItem);
                sqlStatements.add(insertLineItem);
                
                String updateDistribution = "Update distribution_station set sales_sum = sales_sum + " +
                item_price + "where warehouse_id = 1 and " +
                "id = " + distribution_station;
                
                sqlStatements.add(updateDistribution);
                
                //System.out.println("Added " + paymentAmt + " towards the sales sum of distribution center #" + distribution_station + ".");
                
                String updateWarehouse = "Update warehouses set sales_sum = sales_sum + " +
                item_price + "where id = 1";
                
                sqlStatements.add(updateWarehouse);

            }
            
            // Now that we are done with user input and getting the order info,
            // we can run all the statements.
            
            //statement.executeUpdate(startTransaction);
            for (String insertStatement: sqlStatements) {
                //System.out.println("trying: " + insertStatement);
                statement.executeUpdate(insertStatement);
            }
            
            //statement.executeUpdate("COMMIT");
            connection.commit();
            System.out.println("\nTransaction 1 " + "for thread " + m_id +" committed.\n");
            
        } catch(SQLException Ex) {
            try{
                connection.rollback();
                System.out.println("\nTransaction 1 " + "for thread " + m_id +" roll back.\n");
            }
            catch (SQLException e2){
                System.out.println("Error running the sample queries.  Machine Error: " +
                               Ex.toString());
            }
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
    
    public void ExecutePaymentTransaction() {
        Random r = new Random();
        int warehouse_id = 1;
        int distribution_station = r.nextInt(grocery.getNumDistribution()) + 1;
        int custID = r.nextInt(grocery.getNumCustomer()) + 1;
        double paymentAmt = r.nextDouble()*50 + 1;
        paymentTransaction(warehouse_id, distribution_station, custID, paymentAmt);
        return;
    }
    
    public void paymentTransaction(int warehouse_id, int distribution_station, int custID, double paymentAmt) {
        Statement statement = null;
        //int distribution_station = -1;
        //int custID = -1;
        //int warehouse_id = -1;
        //Scanner scan = new Scanner(System.in);
        
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            statement = connection.createStatement();
            //Can start transaction now that we have finished taking user input.
            //String startTransaction = "SET TRANSACTION READ WRITE";
            
            //statement = connection.createStatement();
            //statement.executeUpdate(startTransaction);
            System.out.println("Transaction 2 " + "for thread " + m_id +" started successfully.");
            
            String checkCustomer = "Select outstanding_balance from customers where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and id = " + custID;
            
            resultSet = statement.executeQuery(checkCustomer);
            double outstanding_balance = 0;
            
            if (resultSet.next()) {
                outstanding_balance = resultSet.getDouble(1);
                //System.out.println("Record found for this customer: Outstanding balance = " + outstanding_balance + " prior to payment.");
                
                // If outstanding balance is already 0, don't need to pay towards it.
                if (outstanding_balance == 0) {
                    //System.out.println("Outstanding balance is 0, thus no payments need be applied. Ending transaction and returning now.");
                    //statement.executeUpdate("COMMIT");
                    connection.commit();
                    return;
                }
                
                // We will not allow a negative outstanding balance (credit).
                // So we only pay to the point where we can get the outstanding balance to zero.
                if (paymentAmt > outstanding_balance) {
                    
                    //System.out.println("Outstanding balance is only " + outstanding_balance +
                    //                   ", while payment amount is " + paymentAmt + ". Thus, will only apply " +
                   //                    outstanding_balance + " to the outstanding balance to bring it to zero.");
                    
                    paymentAmt = outstanding_balance;
                }
            }
            else {
                //System.out.println("No record for this customer. Ending transaction and returning.");
                //statement.executeUpdate("COMMIT");
                connection.commit();
                return;
            }
            
            
            //Can start transaction now that we have finished taking user input.
            
            ArrayList <String> sqlStatements = new ArrayList <String> ();
            
            String updateCustomer = "Update customers set outstanding_balance = " +
            (outstanding_balance - paymentAmt) + " where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and id = " + custID;
            
            statement.executeUpdate(updateCustomer);
            
            //System.out.println("\nPaid " + paymentAmt + " towards the outstanding balance in the customers table.");
            
            String updateYearSpent = "update customers set year_spend = year_spend + " + paymentAmt + " where warehouse_id = 1 and " + "distributor_id = " + distribution_station + " and id = " + custID;
            statement.executeUpdate(updateYearSpent);
            
            //System.out.println("Added " + paymentAmt + " towards the year spend amount in the customers table.");
            
            //System.out.println("Added " + paymentAmt + " towards the sales sum of warehouse #1.\n");
            
            //statement.executeUpdate("COMMIT");
            connection.commit();
            System.out.println("\nTransaction 2 " + "for thread " + m_id +" committed.\n");
            
        } catch(SQLException Ex) {
            try{
                connection.rollback();
                System.out.println("Transaction 2 " + "for thread " + m_id +" roll back.");
            }
            catch (SQLException e2){
                System.out.println("Error running the sample queries.  Machine Error: " +
                                   Ex.toString());
            }
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
    
    public void ExecuteOrderStatusTransaction() {
        Random r = new Random();
        int warehouse_id = 1;
        int distribution_station = r.nextInt(grocery.getNumDistribution()) + 1;
        int custID = r.nextInt(grocery.getNumCustomer()) + 1;
        double paymentAmt = r.nextDouble()*50 + 1;
        orderStatusTransaction(warehouse_id, distribution_station, custID);
        return;
    }
    
    public void orderStatusTransaction(int warehouse_id, int distribution_station, int custID) {
        Statement statement = null;
        //int distribution_station = -1;
        //int custID = -1;
        //int warehouse_id = -1;
        //int order_id = -1;
        //Scanner scan = new Scanner(System.in);
        
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            statement = connection.createStatement();
            //Now that we done collecting user input, we can start the transaction.
            //String startTransaction = "SET TRANSACTION READ WRITE";
            
            //statement = connection.createStatement();
            //statement.executeUpdate(startTransaction);
            System.out.println("Transaction 3 " + "for thread " + m_id +" started successfully.");
            
            String checkOrder = "Select id from orders where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and custID = " + custID +
            " and order_date = (select max(order_date) from orders where warehouse_id = 1 and " +
            "distributor_id = " + distribution_station + " and custID = " + custID + ") and rownum = 1";
            
            resultSet = statement.executeQuery(checkOrder);
            int orderID = -1;
            
            if (resultSet.next()) {
                orderID = resultSet.getInt(1);
                //System.out.println("Using orderID = " + orderID);
            }
            else {
                //System.out.println("No record for this customer.");
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
            
            //statement.executeUpdate("COMMIT");
            connection.commit();
            System.out.println("\nTransaction 3 " + "for thread " + m_id +" committed.\n");
            
        } catch(SQLException Ex) {
            try{
                connection.rollback();
                System.out.println("Transaction 3 " + "for thread " + m_id +" roll back.");
            }
            catch (SQLException e2){
                System.out.println("Error running the sample queries.  Machine Error: " +
                                   Ex.toString());
            }
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
    
    public void ExecuteDeliveryTransaction() {
        int warehouse_id = 1;
        deliveryTransaction(warehouse_id);
        return;
    }
    
    public void deliveryTransaction(int warehouse_id) {
        Statement statement = null;
        //int warehouse_id = -1;
        //int distribution_id = -1;
        //Scanner scan = new Scanner(System.in);
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            statement = connection.createStatement();
            // Now that we are done collecting user input, we can start the transaction
            
            //String startTransaction = "SET TRANSACTION READ WRITE";
            //statement = connection.createStatement();
            //statement.executeUpdate(startTransaction);
            System.out.println("Transaction 4 " + "for thread " + m_id +" started successfully.");
            
            //Set up discount array for customers.
            String totalCustomers = "Select count(*) from customers";
            resultSet = statement.executeQuery(totalCustomers);
            resultSet.next();
            double[][] discounts = new double [grocery.getNumDistribution()+1][grocery.getNumCustomer()+1];
            
            //Fill in discount array for customers
            String findDiscounts = "Select distributor_id, id, discount from customers";
            resultSet = statement.executeQuery(findDiscounts);
            int count_customer = 1;
            while (resultSet.next()) {
                discounts[resultSet.getInt(1)][resultSet.getInt(2)] = Math.round(resultSet.getDouble(3)*100.0)/100.0;
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
                
                //System.out.println("\nProcessing the following order: \n-----------------");
                //System.out.println("Warehouse ID = " + warehouseID);
                //System.out.println("Distributor ID = " + distributorID);
                //System.out.println("Customer ID = " + custID);
                //System.out.println("Order ID = " + orderID);
                //System.out.println("Customer Discount = " + discounts[distributorID][custID] + "%");
                
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
                    
                    //System.out.println("\nLine Item Information: \n----------------------");
                    //System.out.println("Warehouse ID = " + warehouseID);
                    //System.out.println("Distributor ID = " + distributorID);
                    //System.out.println("Customer ID = " + custID);
                    //System.out.println("Order ID = " + orderID);
                    //System.out.println("Line Item ID = " + lineItemID);
                    //System.out.println("Item ID = " + itemID);
                    //System.out.println("Quantity = " + quan);
                    //System.out.println("Total Price for " + quan + " items = " + price);
                    //System.out.println("Customer Discount = " + discounts[distributorID][custID] + "%");
                    //System.out.println("Total w/ Discount = " + Math.round(100.0*((1.0-discounts[distributorID][custID]/100.0)*price))/100.0 + "\n");
                    
                    double lineItemTotal = Math.round(100.0*(1.0-discounts[distributorID][custID]/100.0)*price)/100.0;
                    
                    //Make sure it's in stock.
                    String checkInStock = "select quantity_in_stock from warehouse_stock where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                    resultSet3 = statement3.executeQuery(checkInStock);
                    resultSet3.next();
                    int quantityInStock = resultSet3.getInt(1);
                    
                    //not in stock
                    if (quantityInStock < quan) {
                        //System.out.println("Item #" + itemID + " is currently out of stock for this quantity in warehouse #" + warehouseID + ", so it cannot be delivered right now.\n");
                        orderCompleted = false;
                    }
                    //in stock. Now will deliver the line item and charge accordingly.
                    else {
                        
                        //Now charging for the item, so increasing the outstanding balance
                        String updateBalance = "update customers set outstanding_balance = outstanding_balance + " + lineItemTotal + " where warehouse_id = " + warehouseID + " and distributor_id = " + distributorID + " and id = " + custID;
                        statement3.executeUpdate(updateBalance);
                        //System.out.println("Increased the outstanding balance by " + lineItemTotal + " for customer with id " + custID + " and warehouse with id " + warehouseID + " and distribution station with id " + distributorID);
                        
                        //Increasing the number of deliveries by the # of items delivered in this line item.
                        String updateNumDeliveries = "update customers set num_deliveries = num_deliveries + " + quan + " where warehouse_id = " + warehouseID + " and distributor_id = " + distributorID + " and id = " + custID;
                        statement3.executeUpdate(updateNumDeliveries);
                        //System.out.println("Increased the number of deliveries by " + quan + " for customer with id " + custID + " and warehouse with id " + warehouseID + " and distribution station with id " + distributorID);
                        
                        String updateStockQuantity = "update warehouse_stock set quantity_in_stock = quantity_in_stock - " + quan + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                        statement3.executeUpdate(updateStockQuantity);
                        //System.out.println("Decreased stock of this item in warehouse by " + quan);
                        
                        String updateStockSold = "update warehouse_stock set quantity_sold = quantity_sold + " + quan + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                        statement3.executeUpdate(updateStockSold);
                        //System.out.println("Increased quantity of sold for this item in warehouse by " + quan);
                        
                        String updateStockOrder = "update warehouse_stock set number_orders = number_orders + 1" + " where warehouse_id = " + warehouseID + " and item_id = " + itemID;
                        statement3.executeUpdate(updateStockOrder);
                        //System.out.println("Increased number of orders for this item in warehouse by 1.");
                        
                        String setDelivered = "update LineItems set date_delivered = " + "SYSDATE" +
                        " where warehouse_id = " + warehouse_id + " and distributor_id = " + distributorID +
                        " and custID = " + custID + " and order_id = " + orderID + " and id = " + lineItemID;
                        statement3.executeUpdate(setDelivered);
                        //System.out.println("Set the delivery date of this line item to the current date.\n");
                        
                        
                    }
                } // done looping through all line items of a given order
                
                // if all the pending line items were in stock, then we have delivered
                // all and can mark the order as completed.
                if (orderCompleted == true) {
                    String orderComplete = "update orders set completed = 1 where warehouse_id = " +
                    warehouseID + " and distributor_id = " + distributorID + " and custID = " +
                    custID + " and id = " + orderID;
                    statement3.executeUpdate(orderComplete);
                    //System.out.println("Marked the order as complete.\n");
                }
                else {
                    //System.out.println("This order is still incomplete, as certain items are out of stock.\n");
                }
            }
            statement3.close();
            statement2.close();
            
            //statement.executeUpdate("COMMIT");
            connection.commit();
            System.out.println("\nTransaction 4 " + "for thread " + m_id +" committed.\n");
            
        } catch(SQLException Ex){
            try{
                connection.rollback();
                System.out.println("Transaction 4 " + "for thread " + m_id +" roll back.");
            }
            catch (SQLException e2){
                System.out.println("Error running the sample queries.  Machine Error: " +
                                   Ex.toString());
            }
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
    
    public void ExecuteStockLevelTransaction() {
        Random r = new Random();
        int warehouse_id = 1;
        int distribution_station = r.nextInt(grocery.getNumDistribution()) + 1;
        int stock_threshold = r.nextInt(50) + 20;
        stockLevelTransaction(warehouse_id, distribution_station, stock_threshold);
        return;
    }
    
    public void stockLevelTransaction(int warehouse_id, int distribution_station, int stock_threshold) {
        Statement statement = null;
        //int distribution_station = -1;
        //int warehouse_id = -1;
        //int stock_threshold = -1;
        //Scanner scan = new Scanner(System.in);
        
        try {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            statement = connection.createStatement();
            //String startTransaction = "SET TRANSACTION READ WRITE";
            //statement = connection.createStatement();
            //statement.executeUpdate(startTransaction);
            
            //The 20 most recent orders for the distribution station
            System.out.println("Transaction 5 " + "for thread " + m_id +" started successfully.");
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
            
            //System.out.println("There are a total of " + cntResult + " items that have a "
            //                   + "stock under the threshold in the distribution station's warehouse.");
            //System.out.println("They are items: " + itemsQualifying);
            
            //statement.executeUpdate("COMMIT");
            connection.commit();
            System.out.println("\nTransaction 5 " + "for thread " + m_id +" committed.\n");
            
        } catch(SQLException Ex) {
            try{
                connection.rollback();
                System.out.println("Transaction 5 " + "for thread " + m_id +" roll back.");
            }
            catch (SQLException e2){
                System.out.println("Error running the sample queries.  Machine Error: " +
                                   Ex.toString());
            }
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
        String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
        
        try{
            //initialize the database
            grocery = new GroceryDelivery(url, username, password);
            
            //This is the location of the database.  This is the database in oracle
            //provided to the class
            
            //create a connection to DB on class3.cs.pitt.edu
            connection = DriverManager.getConnection(url, username, password);
            //connection.setAutoCommit(false);
            //GroceryDelivery test = new GroceryDeliveryThread();
            //grocery = new GroceryDelivery();
            //thread
            Thread[] threadList = new Thread[NUM_OF_THREADS];
            for (int i = 0; i < NUM_OF_THREADS; i++){
                threadList[i] = new jdbcTransactionThread(i/3);
                threadList[i].start();
            }
            for (int i = 0; i < NUM_OF_THREADS; i++){
                threadList[i].join();
            }
        }
        catch(Exception Ex)  {
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
