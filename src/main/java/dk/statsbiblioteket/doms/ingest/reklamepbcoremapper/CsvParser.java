package dk.statsbiblioteket.doms.ingest.reklamepbcoremapper;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Parse a line from CSV format
 */
public final class CsvParser {
    private static final String CELL_DELIMINATOR = ",";
    private static final String ROW_DELIMINATOR = "\n";
    private static final String QUOTE = "\"";
    private static final String DELIMINATORS = CELL_DELIMINATOR + ROW_DELIMINATOR + QUOTE;

    private enum ParseState {NORMAL, QUOTED_STRING}

    /**
     * Read a CSV file into a list of rows consisting of a list of cells consisting of strings.
     * Cells are deliminated by {@link #CELL_DELIMINATOR}, rows are deliminated by {@link #ROW_DELIMINATOR},
     * cell contents can be quoted with {@link #QUOTE}, two {@link #QUOTE} in a row are interpreted as a literal quote.
     *
     * @param csvdata CSV formatted data.
     * @return A list of rows consisting of a list of cells consisting of strings.
     */
    public static List<List<String>> readCsvData(String csvdata) {
        StringTokenizer tokenizer = new StringTokenizer(csvdata, DELIMINATORS, true);
        ParseState parseState = ParseState.NORMAL;
        List<List<String>> result = new ArrayList<List<String>>();
        List<String> currentRow = new ArrayList<String>();
        String currentCell = "";
        String next;
        while (tokenizer.hasMoreElements()) {
            next = tokenizer.nextToken();
            if (next.equals(QUOTE)) {
                switch(parseState) {
                    case NORMAL:
                        parseState = ParseState.QUOTED_STRING;
                        break;
                    case QUOTED_STRING:
                        parseState = ParseState.NORMAL;
                        break;
                }
            } else if (next.equals(CELL_DELIMINATOR) && parseState == ParseState.NORMAL) {
                currentRow.add(currentCell);
                currentCell = "";
            } else if (next.equals(ROW_DELIMINATOR) && parseState == ParseState.NORMAL) {
                currentRow.add(currentCell);
                currentCell = "";
                result.add(currentRow);
                currentRow = new ArrayList<String>();
            } else {
                currentCell += next;
            }
        }
        if (!currentCell.isEmpty()) {
            currentRow.add(currentCell);
        }
        if (!currentRow.isEmpty()) {
            result.add(currentRow);
        }
        return result;
    }
}
