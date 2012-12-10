package dk.statsbiblioteket.doms.ingest.reklamepbcoremapper;

import java.io.File;

/**
 * Map a file with a utf-8 encoded csv file with TV2 tv commercial metadata to PBCore files.
 */
public class Tv2PBCoreMapperUtil {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java " + Tv2PBCoreMapperUtil.class.toString() + " <csvfile> [outputdir]");
            System.exit(1);
        }

        File outputdir;
        if (args.length < 2 || args[1] == null || args[1].isEmpty()) {
            outputdir = new File(".");
        } else {
            outputdir = new File(args[1]);
        }
        outputdir.mkdirs();
        new Tv2PBCoreMapper().mapCsvDataToPBCoreFiles(new File(args[0]), outputdir);
    }
}
