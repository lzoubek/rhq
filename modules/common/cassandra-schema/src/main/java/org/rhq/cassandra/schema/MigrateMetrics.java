package org.rhq.cassandra.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author John Sanda
 */
public class MigrateMetrics {

    private static final Log log = LogFactory.getLog(MigrateMetrics.class);

    public static void main(String[] args) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        SchemaManager schemaManager = null;
        try {
            log.info(System.getProperty("rhq.storage.nodes"));

            Properties properties = new Properties();
            properties.put(SchemaManager.RELATIONAL_DB_CONNECTION_FACTORY_PROP, new DBConnectionFactory() {
                @Override
                public Connection newConnection() throws SQLException {
                    return null;
                }
            });
            properties.put(SchemaManager.DATA_DIR, "target");
            String[] nodes = System.getProperty("rhq.storage.nodes").split(",");
            schemaManager = new SchemaManager("rhqadmin", "1eeb2f255e832171df8592078de921bc", nodes,
                9042);
            schemaManager.install(properties);
        } catch (Exception e) {
            log.warn("Schema installation failed", e);
        } finally {
            if (schemaManager != null) {
                schemaManager.shutdown();
            }
            stopwatch.stop();
            log.info("Finished schema upgrade in " + stopwatch.elapsed(TimeUnit.MINUTES) + " minutes");
        }
    }
    
}
