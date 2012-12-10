package dk.statsbiblioteket.doms.ingest.reklamepbcoremapper;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Map data in a database with cinematic commercial metadata to PBCore files.
 */
public class BiografPBCoreMapperUtil {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java " + Tv2PBCoreMapperUtil.class.toString() + " <propertiesfile> [outputdir]");
            System.exit(1);
        }

        File outputdir;
        if (args.length < 2 || args[1] == null || args[1].isEmpty()) {
            outputdir = new File(".");
        } else {
            outputdir = new File(args[1]);
        }
        Properties properties = new Properties();
        properties.load(new FileInputStream(args[0]));
        outputdir.mkdirs();
        // Load database driver
        Class.forName(properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbdriver"));
        Connection c = DriverManager
                .getConnection(properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dburl"),
                               properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbuser"),
                               properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbpass"));
        c.setReadOnly(true);
        new BiografPBCoreMapper().mapSQLDataToPBCoreFiles(outputdir, c);
    }
}
