package dk.statsbiblioteket.doms.ingest.reklamepbcoremapper;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test file generations.
 */
public class BiografPBCoreMapperTest {

    private static final File OUTPUTDIR = new File("target/testoutput");

    @Before
    public void setUp() {
        OUTPUTDIR.mkdirs();
    }

    @After
    public void tearDown() {
        for (File file : OUTPUTDIR.listFiles()) {
            file.delete();
        }
        OUTPUTDIR.delete();
    }

    @Test
    public void testMapCsvDataToPBCoreFiles() throws Exception {
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("reklamefilm.properties"));
        Class.forName(properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbdriver"));
        Connection c = DriverManager
                .getConnection(properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dburl"),
                               properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbuser"),
                               properties.getProperty("dk.statsbiblioteket.doms.ingest.reklamefilm.dbpass"));
        c.setReadOnly(true);
        new BiografPBCoreMapper().mapSQLDataToPBCoreFiles(OUTPUTDIR, c);
        File[] generatedFiles = OUTPUTDIR.listFiles();
        assertEquals(3582, generatedFiles.length);
        for (File file : generatedFiles) {
            Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
            assertTrue(d.getElementsByTagName("*").getLength() > 60);
            //TODO Test stuff
        }
    }
}
