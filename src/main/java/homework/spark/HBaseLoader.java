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
    private static final TableName personDataTableName = TableName.valueOf("PersonalDataCounts");
    private static final String family = "PersonalData";
    private Configuration config;
    private Admin admin;
    private Connection connection;
    private Table personDataTable;
    private int rowNumber = 1;

    private static final byte[] firstNameQualifier = Bytes.toBytes("firstName");
    private static final byte[] lastNameQualifier = Bytes.toBytes("lastName");
    private static final byte[] locationQualifier = Bytes.toBytes("location");
    private static final byte[] countQualifier = Bytes.toBytes("count");

    public HBaseLoader() throws IOException {
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
        p.addImmutable(family.getBytes(), firstNameQualifier, Bytes.toBytes(data.firstName));
        p.addImmutable(family.getBytes(), lastNameQualifier, Bytes.toBytes(data.lastName));
        p.addImmutable(family.getBytes(), locationQualifier, Bytes.toBytes(data.location));
        p.addImmutable(family.getBytes(), countQualifier, Bytes.toBytes(data.count));
        personDataTable.put(p);
    }

    private void dropAndCreateTable() throws IOException {
        if (admin.tableExists(personDataTableName)) {
            admin.disableTable(personDataTableName);
            admin.deleteTable(personDataTableName);
        }

        HTableDescriptor desc = new HTableDescriptor(personDataTableName);
        desc.addFamily(new HColumnDescriptor(family));
        admin.createTable(desc);

        personDataTable = connection.getTable(personDataTableName);
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
