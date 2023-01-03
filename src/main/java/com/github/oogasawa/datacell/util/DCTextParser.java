package com.github.oogasawa.datacell.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;
import com.github.oogasawa.datacell.mem.MemDBFactory;
import com.github.oogasawa.utility.files.FileIO;
import com.github.oogasawa.utility.types.string.StringUtil;

import org.apache.commons.configuration.ConfigurationException;


/**
 * DCText形式のファイルを読んで、データベースにデータを入れる
 */
public class DCTextParser {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    String currentDataSet = null;

    Pattern pDataSet = Pattern.compile("^@\\s*(ds|data set)\\s*:\\s*(.+)");
    Pattern pID = Pattern.compile("^@\\s*(id|ID)\\s*:\\s*(.+)");
    Pattern pPredicate = Pattern.compile("^@[@+]\\s*(.+?)\\s*:\\s*(.+)");
    Pattern pHead = Pattern.compile("^@[@+]\\s*([^:]+?)\\s*$");
    Pattern pTail = Pattern.compile("^@[@+]\\s*end\\s*$");

    Pattern pDB = Pattern.compile("^@\\$\\s*(DB|db)\\s*:\\s(.+)");
    Pattern pDS = Pattern.compile("^@\\$\\s*(ds|data set)\\s*:\\s(.+)");


    DataCell datacell = null;

    String ds = null; // A data set.
    String id = null; // An identifier.
    String pred = null;
    StringBuilder sb = null;

    int state = 0;


    public static void main(String[] argv) {
        String fname = null;
        String dbName = null;

        if (argv.length == 0) {
            fname = "/home/oogasawa/tmp/test.dctext";
            dbName = "test_doc";
        } else if (argv.length == 1) {
            fname = argv[0];
            dbName = null;
        } else {
            fname = argv[0];
            dbName = argv[1];
        }

        DCContainerFactory facObj = new MemDBFactory();
        DCTextParser obj = new DCTextParser();
        //obj.setCurrentDataSet("DataCell_" + fname);
        obj.parse(new File(fname), new MemDBFactory(), dbName);

    }

    public void parse(String dctext, DCContainerFactory facObj, String dbName) {

        DCContainer dbObj = null;
        try {

            if (dbName != null) {
                facObj.createDBIfAbsent(dbName);
                dbObj = facObj.getInstance(dbName);

            }

            logger.info("dbName : " + dbName);
            logger.info("facObj : " + facObj);
            logger.info("dbObj  : " + dbObj);
            logger.info("dctext : " + dctext);

            ArrayList<String> lines = StringUtil.splitByNewLine(dctext);
            for (String line : lines) {
                if (line.startsWith("@END@")) {
                    break;
                }
                analyzeLine(line, facObj, dbObj);
            }

        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.datacell.util.DCTextParser", "parse", e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
        }

    }

    public void parse(ArrayList<String> lines, DCContainerFactory facObj, String dbName) {
        DCContainer dbObj = null;
        try {

            if (dbName != null) {
                facObj.createDBIfAbsent(dbName);
                dbObj = facObj.getInstance(dbName);

            }

            logger.info("dbName : " + dbName);
            logger.info("facObj : " + facObj);
            logger.info("dbObj  : " + dbObj);

            for (String line : lines) {

                if (line.startsWith("@END@")) {
                    break;
                }

                analyzeLine(line, facObj, dbObj);
            }

        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.datacell.util.DCTextParser", "parse", e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
        }

    }

    public void parse(File fObj, DCContainerFactory facObj, String dbName) {

        DCContainer dbObj = null;
        BufferedReader br = null;
        try {

            if (dbName != null) {
                facObj.createDBIfAbsent(dbName);
                dbObj = facObj.getInstance(dbName);

            }

            br = FileIO.getBufferedReader(fObj);
            String line = null;
            int state = 0; // out of any predicate blocks.
            while ((line = br.readLine()) != null) {
                if (line.startsWith("@END@")) {
                    break;
                }

                analyzeLine(line, facObj, dbObj);
            }

        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.datacell.util.DCTextParser", "parse", e);
        } catch (IOException e) {
            logger.throwing("com.github.oogasawa.datacell.util.DCTextParser", "parse", e);
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    logger.throwing("com.github.oogasawa.datacell.util.DCTextParser", "parse", e);
                }
            }
        }

    }

    public void analyzeLine(String line, DCContainerFactory facObj, DCContainer dbObj) throws ConfigurationException {

        logger.fine("line: " + line);

        if (line == null) {
            logger.fine("line is null.");
            return;
        } else if (isDataSet(line)) {
            logger.fine("isDataSet");

            state = 0;

            ds = getDataSet(line);
            // if (ds.equals("--") || ds.equals("---")) { // not specified.
            // ds = getCurrentDataSet();
            // }
            datacell = new DataCell();
            datacell.setDataSet(ds);
        } else if (isID(line)) {
            logger.fine("isID");

            state = 0;

            id = getID(line);
            // rootDoc.add(id);
            // if (id.equals("--") || id.equals("---")) { // not specified.
            // id = generateID();
            // }
        } else if (isPredicate(line)) {
            logger.fine("isPredicate");

            state = 0;

            ArrayList<String> kv = getPredAndValue(line);
            logger.fine(ds + ", " + id + ", " + kv.get(0) + ", " + kv.get(1));
            if (overwrite(line)) {
                dbObj.putRowWithReplacingValues(ds, id, kv.get(0), kv.get(1));
            } else {
                dbObj.putRowIfKeyValuePairIsAbsent(ds, id, kv.get(0), kv.get(1));
            }
        } else if (isTailOfBlock(line)) {
            logger.fine("isTailOfBlock");

            state = 0;
            logger.fine(ds + ", " + id + ", " + pred + ", " + sb.toString());
            if (overwrite(line)) {
                dbObj.putRowWithReplacingValues(ds, id, pred, sb.toString());
            } else {
                dbObj.putRowIfKeyValuePairIsAbsent(ds, id, pred, sb.toString());
            }
        } else if (isHeadOfBlock(line)) {
            logger.fine("isHeadOfBlock");

            state = 1;
            pred = getPredicate(line);
            sb = new StringBuilder();

        } else if (state != 0) { // in a predicate block.
            logger.fine("state != 0 ... this line is in a predicate block.");

            sb.append(line + "\n");
        } else if (isDB(line)) {
            logger.fine("isDB");

            if (dbObj != null) {
                dbObj.close();
            }
            String name = getDBName(line);
            facObj.createDBIfAbsent(name);
            dbObj = facObj.getInstance(name);

        } else if (isCurrentDataSet(line)) {
            logger.fine("isCurrentDataSet");
            String cds = getDS(line);
            setCurrentDataSet(cds);
        } else {
            logger.fine("else!");
            // do nothing.
        }
    }

    public boolean overwrite(String line) {
        if (line.startsWith("@@")) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * public void makeCellOrderCell(DCContainer dbObj, String ds, String id, String
     * pred) { DataCell dc = new DataCell(); dc.setDataSet(ds);
     * dc.setID("DUCKBILL_ROOT"); dc.setPredicate("cell order");
     * //dc.setValue(ListUtil.join("\n", rootDoc));
     * 
     * dbObj.putRowIfKeyValuePairIsAbsent(ds, dc.getID(), dc.getPredicate(),
     * dc.getValue()); }
     * 
     * 
     * public void makeCellOrderCell(DCContainer dbObj, String ds, String id) {
     * makeCellOrderCell(dbObj, ds, id, "cell order"); }
     * 
     * public void makeCellOrderCell(DCContainer dbObj, String ds) {
     * makeCellOrderCell(dbObj, ds, "root_cell_order_cell", "cell order"); }
     */
    private String getPredicate(String line) {
        String result = null;
        Matcher m = pHead.matcher(line);
        if (m.find()) {
            result = m.group(1);
        }
        return result;
    }

    private ArrayList<String> getPredAndValue(String line) {
        ArrayList<String> result = null;
        Matcher m = pPredicate.matcher(line);
        if (m.find()) {
            result = new ArrayList<String>();
            result.add(m.group(1));
            result.add(m.group(2));
        }
        return result;
    }

    private String generateID() {
        return UUID.randomUUID().toString();
    }

    public String getID(String line) {
        String result = null;
        Matcher m = pID.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    private String getCurrentDataSet() {
        return currentDataSet;
    }

    private void setCurrentDataSet(String ds) {
        currentDataSet = ds;
    }

    /**
     *
     * @param line チェックする文字列
     * @return 検出されたデータセット名
     */
    public String getDataSet(String line) {
        String result = null;
        Matcher m = pDataSet.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    private String getDS(String line) {
        String result = null;
        Matcher m = pDS.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    private String getDBName(String line) {
        String result = null;
        Matcher m = pDB.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    public boolean isDB(String line) {
        boolean result = false;
        Matcher m = pDB.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isDataSet(String line) {
        boolean result = false;
        Matcher m = pDataSet.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isID(String line) {
        boolean result = false;
        Matcher m = pID.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isCurrentDataSet(String line) {
        boolean result = false;
        Matcher m = pDS.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isPredicate(String line) {

        boolean result = false;
        Matcher m = pPredicate.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isHeadOfBlock(String line) {

        boolean result = false;
        Matcher m = pHead.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

    public boolean isTailOfBlock(String line) {

        boolean result = false;
        Matcher m = pTail.matcher(line);
        if (m.find()) {
            logger.fine(line);
            result = true;
        }
        return result;
    }

}
