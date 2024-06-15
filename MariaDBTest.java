import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class MariaDBTest {

    private static final String driver = "org.mariadb.jdbc.Driver";
    private static final String url = "jdbc:mariadb://0.tcp.jp.ngrok.io:11051/411177029";
    private static final String user = "411177029";
    private static final String password = "411177029";

    public static void main(String[] args) {
        try {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(url, user, password);
            System.out.println("Success: Connected to the database!");

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println("\nQueries:");
                System.out.println("1. Find the VIN of each car containing defective transmission made by supplier Getrag between two given dates, and the customer to which it was sold.");
                System.out.println("2. Find the dealer who has sold the most (by dollar-amount) in the past year.");
                System.out.println("3. Find the top 2 brands by unit sales in the past year.");
                System.out.println("4. In what month(s) do SUVs sell best?");
                System.out.println("5. Find those dealers who keep a vehicle in inventory for the longest average time.");
                System.out.println("6. Show table row counts.");
                System.out.println("7. Exit");

                int choice = scanner.nextInt();
                switch (choice) {
                    case 1:
                        findDefectiveTransmissions(connection);
                        break;
                    case 2:
                        findTopDealerBySales(connection);
                        break;
                    case 3:
                        findTopBrandsBySales(connection);
                        break;
                    case 4:
                        findBestSellingMonthsForSUVs(connection);
                        break;
                    case 5:
                        findDealersWithLongestInventoryTime(connection);
                        break;
                    case 6:
                        showTableRowCounts(connection);
                        break;
                    case 7:
                        connection.close();
                        System.out.println("Disconnected from the database.");
                        scanner.close();
                        return;
                    default:
                        System.out.println("Invalid choice, please select again.");
                }
            }

        } catch (ClassNotFoundException e) {
            System.err.println("Error: MariaDB JDBC driver not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error: Connection failed!");
            e.printStackTrace();
        }
    }

    private static void findDefectiveTransmissions(Connection connection) throws SQLException {
        String query = "SELECT v.VIN, c.name AS CustomerName, tm.manufacture_date " +
                       "FROM vehicles v " +
                       "JOIN transmission_manufacture tm ON v.transmission_id = tm.transmission_id " +
                       "JOIN customers c ON v.customer_id = c.customer_id " +
                       "WHERE tm.supplier_id = (SELECT supplier_id FROM suppliers WHERE supplier_name = 'Getrag') " +
                       "AND tm.manufacture_date BETWEEN '2023-01-01' AND '2023-03-01'";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Defective Transmissions:");
        while (resultSet.next()) {
            String vin = resultSet.getString("VIN");
            String customerName = resultSet.getString("CustomerName");
            String manufactureDate = resultSet.getString("manufacture_date");
            System.out.println("VIN: " + vin + ", Customer Name: " + customerName + ", Manufacture Date: " + manufactureDate);
        }

        resultSet.close();
        statement.close();
    }

    private static void findTopDealerBySales(Connection connection) throws SQLException {
        String query = "SELECT d.dealer_name, SUM(s.price) AS TotalSales " +
                       "FROM sales s " +
                       "JOIN dealers d ON s.dealer_id = d.dealer_id " +
                       "WHERE s.sale_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) " +
                       "GROUP BY d.dealer_name " +
                       "ORDER BY TotalSales DESC " +
                       "LIMIT 1";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Top Dealer By Sales:");
        while (resultSet.next()) {
            String dealerName = resultSet.getString("dealer_name");
            double totalSales = resultSet.getDouble("TotalSales");
            System.out.println("Dealer Name: " + dealerName + ", Total Sales: " + totalSales);
        }

        resultSet.close();
        statement.close();
    }

    private static void findTopBrandsBySales(Connection connection) throws SQLException {
        String query = "SELECT b.brand_name, COUNT(s.VIN) AS UnitSales " +
                       "FROM sales s " +
                       "JOIN vehicles v ON s.VIN = v.VIN " +
                       "JOIN brands b ON v.brand_id = b.brand_id " +
                       "WHERE s.sale_date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR) " +
                       "GROUP BY b.brand_name " +
                       "ORDER BY UnitSales DESC " +
                       "LIMIT 2";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Top 2 Brands By Sales:");
        while (resultSet.next()) {
            String brandName = resultSet.getString("brand_name");
            int unitSales = resultSet.getInt("UnitSales");
            System.out.println("Brand Name: " + brandName + ", Unit Sales: " + unitSales);
        }

        resultSet.close();
        statement.close();
    }

    private static void findBestSellingMonthsForSUVs(Connection connection) throws SQLException {
        String query = "SELECT MONTH(s.sale_date) AS SaleMonth, COUNT(v.VIN) AS SUVSales " +
                       "FROM sales s " +
                       "JOIN vehicles v ON s.VIN = v.VIN " +
                       "JOIN models m ON v.model_id = m.model_id " +
                       "WHERE m.body_style = 'SUV' " +
                       "GROUP BY SaleMonth " +
                       "ORDER BY SUVSales DESC " +
                       "LIMIT 1";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Best Selling Months for SUVs:");
        while (resultSet.next()) {
            int saleMonth = resultSet.getInt("SaleMonth");
            int suvSales = resultSet.getInt("SUVSales");
            System.out.println("Month: " + saleMonth + ", SUV Sales: " + suvSales);
        }

        resultSet.close();
        statement.close();
    }

    private static void findDealersWithLongestInventoryTime(Connection connection) throws SQLException {
        String query = "SELECT d.dealer_name, AVG(DATEDIFF(CURDATE(), i.inventory_date)) AS AvgInventoryTime " +
                       "FROM inventory i " +
                       "JOIN dealers d ON i.dealer_id = d.dealer_id " +
                       "GROUP BY d.dealer_name " +
                       "ORDER BY AvgInventoryTime DESC " +
                       "LIMIT 1";

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        System.out.println("Dealers With Longest Inventory Time:");
        while (resultSet.next()) {
            String dealerName = resultSet.getString("dealer_name");
            double avgInventoryTime = resultSet.getDouble("AvgInventoryTime");
            System.out.println("Dealer Name: " + dealerName + ", Average Inventory Time: " + avgInventoryTime);
        }

        resultSet.close();
        statement.close();
    }

    private static void showTableRowCounts(Connection connection) throws SQLException {
        String[] tables = {"vehicles", "options", "customers", "suppliers", "sales", "brands", "models", "inventory", "dealers", "manufacturing_plants", "transmission_manufacture"};
        Statement statement = connection.createStatement();

        System.out.println("Table Row Counts:");
        for (int i = 0; i < tables.length; i++) {
            String query = "SELECT COUNT(*) AS count FROM " + tables[i];
            ResultSet resultSet = statement.executeQuery(query);

            if (resultSet.next()) {
                int count = resultSet.getInt("count");
                System.out.println(tables[i] + ": " + count);
            }

            resultSet.close();
        }

        statement.close();
    }
}