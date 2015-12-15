import oracle.jdbc.OracleStatement;
import java.sql.*;  //import the file containing definitions for the parts
import java.text.ParseException;
import java.util.*;

public class GroceryDelivery{
    
    private Connection connection;
    private Statement statement, statement2;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet, resultSet2;
    
    private int numberWarehouses, numberDistribution, numberCustomers;
    private int numberItems, maxOrdersPerCustomer, minLineItemsPerOrder;
    private int maxLineItemsPerOrder;
    
    public GroceryDelivery(String url, String username, String password) throws SQLException, ClassNotFoundException {
        groceryInitialize(url, username, password);
    }
    
    public void groceryInitialize(String url, String username, String password) throws SQLException, ClassNotFoundException {
        try{
            DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
            //String url = "jdbc:oracle:thin:@class3.cs.pitt.edu:1521:dbclass";
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            createTables();
            populateTables();
            updateAggregateField();
        }
        catch(SQLException Ex){
            System.out.println("Error connecting to database. Machine Error: " + Ex.toString());
        }
        finally{
            connection.close();
        }
        
    }
    
    public int getNumWarehouse(){
        return numberWarehouses;
    }
    
    public int getNumDistribution(){
        return numberDistribution;
    }
    
    public int getNumCustomer(){
        return numberCustomers;
    }
    
    public int getNumItems(){
        return numberItems;
    }
    
    public int getNumMaxOrder(){
        return maxOrdersPerCustomer;
    }
    
    public int getNumMinLineItem(){
        return minLineItemsPerOrder;
    }
    
    public int getNumMaxLineItem(){
        return maxLineItemsPerOrder;
    }
    
    public void updateAggregateField(){
        try{
        	System.out.println("Aggregate field time.");
            String startTransaction = "SET TRANSACTION READ WRITE";
            statement = connection.createStatement();
            statement2 = connection.createStatement();
            String lineItemsOfOrder = "select * from lineItems";
            resultSet2 = statement.executeQuery(lineItemsOfOrder);
            int count = 0;
            while(resultSet2.next()){
                count++;
                //System.out.println("lineItem count: " + count);
                int warehouse_id = resultSet2.getInt(1);
                int distributor_id = resultSet2.getInt(2);
                int cust_id = resultSet2.getInt(3);
                int item_id = resultSet2.getInt(6);
                int quantity = resultSet2.getInt(7);
                double price = resultSet2.getDouble(8);
                //System.out.println("Testing: " + warehouse_id + "/" + distributor_id + "/" + cust_id + "/" + item_id);
                String updateWarehouse = "Update warehouses set sales_sum = sales_sum + " +
                price + "where id = 1";
                statement2.executeUpdate(updateWarehouse);
                String updateDistribution = "Update distribution_station set sales_sum = sales_sum + " +
                price + "where warehouse_id = 1 and " + "id = " + distributor_id;
                statement2.executeUpdate(updateDistribution);
                
                /* Outstanding balance increased only when delivered in transaction 3.4 */
                //String updateCustomer = "update customers set outstanding_balance = outstanding_balance + " + price + " where warehouse_id = 1 and " + "distributor_id = " + distributor_id + " and id = " + cust_id;
                //statement2.executeUpdate(updateCustomer);
                
                String updateStockSold = "update warehouse_stock set quantity_sold = quantity_sold + " + quantity + " where warehouse_id = " + warehouse_id + " and item_id = " + item_id;
                statement2.executeUpdate(updateStockSold);
                String updateStockOrder = "update warehouse_stock set number_orders = number_orders + 1 " + "where warehouse_id = 1 " + "and item_id = " + item_id;
                statement2.executeUpdate(updateStockOrder);
            }
            statement.executeUpdate("COMMIT");
            System.out.println("Committed aggregate field");
        }
        catch (SQLException Ex){
            System.out.println("Error running the queries.  Machine Error: " +
                               Ex.toString());
        }
        finally{
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
            //Scanner scan = new Scanner(System.in);
            numberWarehouses = 1;
            numberDistribution = 5;
            numberCustomers = 50;
            numberItems = 100;
            maxOrdersPerCustomer = 10;
            minLineItemsPerOrder = 5;
            maxLineItemsPerOrder = 10;
            //double warehouseSale  = 0;
            System.out.println("There is one warehouse.");
            System.out.println("There are three distribution stations.");
            System.out.println("There are 10 customers per distribution station.");
            System.out.println("There are a total of 100 items.");
            System.out.println("There are at most 5 orders for each customer.");
            System.out.println("There are at least 3 items for each order and there are at most 7 items for each order.");
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
            double[][] discounts = new double [numberDistribution+1][numberCustomers+1];
            
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
                    double discount = (r.nextDouble()+0.1)*10;
                    discounts[i+1][j+1] = discount;
                    double outstanding_balance = 0;
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
                            
                            //Math.round(100.0*(1.0-discounts[distributorID][custID]/100.0)*price)/100.0;
                    
                            entered.add(item_id);
                            int quantity = r.nextInt(10) + 1;
                            quantityOrderedPerItem[item_id] += quantity;
                            double price = (itemPrice.get(item_id-1)*quantity);
                          	double total = Math.round(100.0*(1.0-discounts[i+1][j+1]/100.0)*price)/100.0;
                            int monthDeliv = r.nextInt(12) + 1;
                            int dayDeliv = r.nextInt(28) + 1;
                            int yearDeliv = r.nextInt(6) + 2010;
                            //String date_delivered = "TO_DATE('" + monthDeliv + "-" + dayDeliv + "-" + yearDeliv + "', 'MM-DD-YYYY')";
                            String date_delivered = "NULL";
                            String insertLineItem = "insert into LineItems values(1," + (i+1) + ", " + (j+1) + ", " + (k+1) + ", " + (m+1) + ", " + item_id + ", " + quantity + ", " + total + ", " + date_delivered + ")";
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
                //int inStock = r.nextInt(200);
                
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
            
            //To update the corresponding aggregate fields when data generation is finished.
            
            statement.executeUpdate("COMMIT");
            //updateAggregateField();
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

}