package homework.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadPhoenix {
    public static void initialize() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        try(Connection connection = DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-unsecure")){
            connection.setAutoCommit(true);

            Statement dropIfExistStatement = connection.createStatement();
            dropIfExistStatement.executeUpdate("DROP TABLE IF EXISTS PersonalDataCounts");

            Statement createTableStatement = connection.createStatement();
            createTableStatement.executeUpdate("" +
                    "CREATE TABLE PersonalDataCounts");
        }

    }
}
