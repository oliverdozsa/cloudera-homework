package homework.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

class PhoenixMappingTable {
    private String tableName;

    public PhoenixMappingTable(String tableName) {
        this.tableName = tableName;
    }

    public void create() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-unsecure")) {
            connection.setAutoCommit(true);
            executeCreate(connection);
        }
    }

    private void executeCreate(Connection connection) throws SQLException {
        try (Statement createTableStatement = connection.createStatement()) {
            createTableStatement.executeUpdate(makeCreateSql());
        }
    }

    private String makeCreateSql(){
        return "" +
                "CREATE TABLE IF NOT EXISTS \"" + tableName + "\" ( " +
                "pk VARCHAR PRIMARY KEY, " +
                "\"PersonalData\".\"firstName\" VARCHAR, " +
                "\"PersonalData\".\"lastName\" VARCHAR, " +
                "\"PersonalData\".\"location\" VARCHAR, " +
                "\"PersonalData\".\"count\" UNSIGNED_INT " +
                ")";
    }
}
