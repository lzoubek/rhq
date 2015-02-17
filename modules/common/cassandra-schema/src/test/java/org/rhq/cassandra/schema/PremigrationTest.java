package org.rhq.cassandra.schema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.testng.annotations.Test;

/**
 * @author John Sanda
 */
public class PremigrationTest extends SchemaUpgradeTest {

    @Test
    public void doPremigrationSteps() throws Exception {
        Properties properties = new Properties();
        properties.put(SchemaManager.RELATIONAL_DB_CONNECTION_FACTORY_PROP, new DBConnectionFactory() {
            @Override
            public Connection newConnection() throws SQLException {
                return newJDBCConnection();
            }
        });
        properties.put(SchemaManager.DATA_DIR, "target");

        SchemaManager schemaManager = new SchemaManager("rhqadmin", "1eeb2f255e832171df8592078de921bc",
            new String[]{"127.0.0.1"}, 9042);
        schemaManager.setUpdateFolderFactory(new TestUpdateFolderFactory(VersionManager.Task.Update.getFolder())
            .removeFiles("0005.xml", "0006.xml", "0007.xml"));
        schemaManager.drop();
        schemaManager.shutdown();
        schemaManager.install(properties);
        schemaManager.shutdown();
    }

    private Connection newJDBCConnection() throws SQLException {
        return DriverManager.getConnection(System.getProperty("rhq.db.url"),
            System.getProperty("rhq.db.username"), System.getProperty("rhq.db.password"));
    }

}
