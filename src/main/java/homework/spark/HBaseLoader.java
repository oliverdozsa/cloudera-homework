package homework.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

class HBaseLoader implements AutoCloseable {
    private static final String FAMILY = "PersonalData";

    private Configuration config;
    private Admin admin;
    private Connection connection;
    private Table personDataTable;
    private int rowNumber = 1;
    private TableName peopleDataTableName;

    private static final byte[] FIRST_NAME_QUALIFIER = Bytes.toBytes("firstName");
    private static final byte[] LAST_NAME_QUALIFIER = Bytes.toBytes("lastName");
    private static final byte[] LOCATION_QUALIFIER = Bytes.toBytes("location");
    private static final byte[] COUNT_QUALIFIER = Bytes.toBytes("count");

    public HBaseLoader(String tableName) throws IOException {
        peopleDataTableName = TableName.valueOf(tableName);
        configureHBase();
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
        dropAndCreateTable();
    }

    public void put(List<Person> people) throws IOException {
        for (Person person : people) {
            put(person);
        }
    }

    private void put(Person data) throws IOException {
        Put p = new Put(Bytes.toBytes("row" + (rowNumber++)));
        p.addImmutable(FAMILY.getBytes(), FIRST_NAME_QUALIFIER, Bytes.toBytes(data.firstName));
        p.addImmutable(FAMILY.getBytes(), LAST_NAME_QUALIFIER, Bytes.toBytes(data.lastName));
        p.addImmutable(FAMILY.getBytes(), LOCATION_QUALIFIER, Bytes.toBytes(data.location));
        p.addImmutable(FAMILY.getBytes(), COUNT_QUALIFIER, Bytes.toBytes(data.count));
        personDataTable.put(p);
    }

    private void dropAndCreateTable() throws IOException {
        if (admin.tableExists(peopleDataTableName)) {
            admin.disableTable(peopleDataTableName);
            admin.deleteTable(peopleDataTableName);
        }

        HTableDescriptor desc = new HTableDescriptor(peopleDataTableName);
        desc.addFamily(new HColumnDescriptor(FAMILY));
        admin.createTable(desc);

        personDataTable = connection.getTable(peopleDataTableName);
    }

    @Override
    public void close() throws Exception {
        personDataTable.close();
        connection.close();
    }

    private void configureHBase() {
        config = HBaseConfiguration.create();

        String path = CountTriplets.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));
    }

}
