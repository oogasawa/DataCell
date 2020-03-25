package net.ogalab.datacell.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ogalab.datacell.DataCell;
import net.ogalab.microutil.container.ListUtil;
import net.ogalab.microutil.container.SetUtil;
import net.ogalab.microutil.type.StringUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 *
 * @author oogasawa
 *
 */
public abstract class AbstractDCContainer implements DCContainer {

    Logger logger = LoggerFactory.getLogger(AbstractDCContainer.class);

    protected NameConverter nameConverter = null;

    protected String iterableTable = null;

    String currentDataSet = null;

    Pattern pDataSet = Pattern.compile("^@\\s*(ds|data set)\\s*:\\s*(.+)");
    Pattern pID = Pattern.compile("^@\\s*(id|ID)\\s*:\\s*(.+)");
    Pattern pPredicate = Pattern.compile("^@@\\s*(.+?)\\s*:\\s*(.+)");
    Pattern pHead = Pattern.compile("^@@\\s*([^:]+?)\\s*$");
    Pattern pTail = Pattern.compile("^@@\\s*end\\s*$");

    Pattern pDB = Pattern.compile("^@\\$\\s*(DB|db)\\s*:\\s(.+)");
    Pattern pDS = Pattern.compile("^@\\$\\s*(ds|data set)\\s*:\\s(.+)");

    ArrayList<String> rootDoc = new ArrayList<String>();

    DataCell datacell = null;

    String ds = null; // A data set.
    String id = null; // An identifier.
    String pred = null;
    StringBuilder sb = null;

    int state = 0;

    protected String[] reservedNames = {
        "internal_name", "original_name"
    };

    private String trimIt(String str) {
        String result = "";
        if (str != null) {
            result = str.trim();
        }

        return result;
    }

    public boolean getAutoCommit() {
        return false;
    }

    public void setAutoCommit(boolean ac) {
        // nothing to do.
    }

    public void commit() {
        // nothing to do.
    }

    public void appendRow(String dataset, String id, String pred, String value) {
        appendRow(nameConverter.makeTableName(dataset, pred), id, value);
    }

    public void appendRow(DataCell cell) {
        appendRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    abstract public void createTable(String tableName);

    public void createTable(String ds, String pred) {
        createTable(nameConverter.makeTableName(ds, pred));
    }

    public void createTable(DataCell cell) {
        createTable(cell.getDataSet(), cell.getPredicate());
    }

    public void createTableIfAbsent(String tableName) {
        //logger.info("createTableIfAbsent: tableName: " + tableName);
        if (!hasTableInAllTables(tableName)) {
            createTable(tableName);
        }
    }

    public void createTableIfAbsent(String ds, String pred) {
        createTableIfAbsent(nameConverter.makeTableName(ds, pred));
    }

    public void createTableIfAbsent(DataCell cell) {
        createTableIfAbsent(cell.getDataSet(), cell.getPredicate());
    }

    public void deleteAllDCTables() {
        ArrayList<String> list = getTableList();
        for (String t : list) {
            deleteTable(t);
        }
    }

    public void deleteID(String ds, String id, String predicate) {
        deleteID(nameConverter.makeTableName(ds, predicate), id);
    }

    public void deleteID(DataCell cell) {
        deleteID(cell.getDataSet(), cell.getID(), cell.getPredicate());
    }

    public void deleteRow(String ds, String id, String pred, String value) {
        deleteRow(nameConverter.makeTableName(ds, pred), id, value);
    }

    public void deleteRow(DataCell cell) {
        deleteRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    abstract public void deleteTable(String tableName);

    public void deleteTable(String ds, String pred) {
        deleteTable(nameConverter.makeTableName(ds, pred));
    }

    public void deleteTable(DataCell cell) {
        deleteTable(cell.getDataSet(), cell.getPredicate());
    }

    public void deleteTableIfExists(String tableName) {
        if (hasTableInAllTables(tableName)) {
            deleteTable(tableName);
        }
    }

    public void deleteTableIfExists(String ds, String pred) {
        deleteTableIfExists(nameConverter.makeTableName(ds, pred));
    }

    public void deleteTableIfExists(DataCell cell) {
        deleteTableIfExists(cell.getDataSet(), cell.getPredicate());
    }

    /*
     public ArrayList<String> getDataSetList() {
     ArrayList<String> result = new ArrayList<String>();
		
     Pattern pDataset = Pattern.compile("^(.+?)__(.+)");
		
     ArrayList<String> tables = getTableList();
     for (String tableName : tables) {
     Matcher m = pDataset.matcher(tableName);
     if (m.matches()) {
     String dataset = m.group(1);
     if (!isReservedName(dataset)) {
     result.add(nameConverter.getOriginalName(dataset));
     }
     }
     }
     return result;
     }
	
	
     public ArrayList<String> getPredicateList(String dataset) {
     ArrayList<String> result = new ArrayList<String>();
		
     Pattern pDataset = Pattern.compile("^" + nameConverter.getInternalName(dataset) + "__(.+)" );
		
     ArrayList<String> tables = getTableList();
     for (String tableName : tables) {
     Matcher m = pDataset.matcher(tableName);
     if (m.matches()) {
     String pred = m.group(1);
     if (!isReservedName(pred)) {
     result.add(nameConverter.getOriginalName(pred));
     }
     }
     }
     return result;
     }
     */
    public DataCell getDataCell(String ds, String id, String pred) {
        DataCell cell = new DataCell();
        cell.setDataSet(ds);
        cell.setID(id);
        cell.setPredicate(pred);
        cell.setValue(getValue(ds, id, pred));

        return cell;
    }

    public ArrayList<DataCell> getDataCellList(String ds, String pred) {

        ArrayList<DataCell> result = new ArrayList<DataCell>();

        for (DataCell cell : this.setIterableTable(ds, pred)) {
            result.add(cell);
        }
        return result;
    }

    public ArrayList<DataCell> getDataCellList(String ds, String id, String pred) {
        ArrayList<DataCell> result = new ArrayList<DataCell>();

        DataCell cell = null;
        ArrayList<String> valueList = getValueList(ds, id, pred);
        for (String value : valueList) {
            cell = new DataCell();
            cell.setDataSet(ds);
            cell.setID(id);
            cell.setPredicate(pred);
            cell.setValue(value);
            result.add(cell);
        }
        return result;
    }

    public ArrayList<ArrayList<String>> getDsPredPairs() {
        ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
        ArrayList<String> tableList = getTableList();

        for (String tableName : tableList) {
            ArrayList<String> pair0 = nameConverter.parseTableName(tableName);
            ArrayList<String> pair = new ArrayList<String>();
            if (pair0.size() >= 2) {
                String ds = nameConverter.getOriginalName(pair0.get(0));
                String pd = nameConverter.getOriginalName(pair0.get(1));
                if (ds != null && pd != null) {
                    pair.add(ds);
                    pair.add(pd);
                    result.add(pair);
                }
            }
        }
        return result;
    }

    public ArrayList<String> getTableList() {
        ArrayList<String> result = new ArrayList<String>();
        ArrayList<String> tableList = getListOfAllTables();
        for (String t : tableList) {
            if (isDataCellTable(t)) {
                result.add(t);
            }
        }
        return result;
    }

    public boolean isDataCellTable(String tableName) {
        ArrayList<String> name = nameConverter.parseTableName(tableName);
        if (name.size() == 2 && nameConverter.hasOriginalName(name.get(0))
                && nameConverter.hasOriginalName(name.get(1))) {
            return true;
        } else {
            return false;
        }
    }

    public ArrayList<String> getValueList(String ds, String id, String pred) {
        return getValueList(nameConverter.makeTableName(ds, pred), id);
    }

    public boolean hasID(String ds, String id, String pred) {
        return hasID(nameConverter.makeTableName(ds, pred), id);
    }

    public boolean hasID(DataCell cell) {
        return hasID(cell.getDataSet(), cell.getID(), cell.getPredicate());
    }

    public boolean hasRow(String ds, String id, String pred, String value) {
        return hasRow(nameConverter.makeTableName(ds, pred), id, value);
    }

    public boolean hasRow(DataCell cell) {
        return hasRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    public boolean hasTableInAllTables(String tableName) {
        boolean result = false;
        ArrayList<String> list = getListOfAllTables();
        for (String t : list) {
            if (t.equalsIgnoreCase(tableName)) {
                result = true;
                break;
            }
        }
        return result;
    }

    public boolean hasTable(String ds, String pred) {
        return hasTableInAllTables(nameConverter.makeTableName(ds, pred));
    }

    public boolean hasTable(DataCell cell) {
        return hasTable(cell.getDataSet(), cell.getPredicate());
    }

    @SuppressWarnings("unused")
    private boolean isReservedName(String name) {
        boolean result = false;
        for (String n : reservedNames) {
            if (name.equals(n)) {
                result = true;
                break;
            }
        }
        return result;
    }

	//abstract public Iterator<DataCell> iterator();
    public void putRow(String tableName, String id, String value) {
        putRow(tableName, id, value, false);
    }

    public void putRow(String tableName, String id, String value, boolean trim) {
        if (trim == true) {
            id = trimIt(id);
            value = trimIt(value);
        }
        createTableIfAbsent(tableName);
        appendRow(tableName, id, value);
    }

    public void putRow(String ds, String id, String pred, String value) {
        putRow(ds, id, pred, value, false);
    }

    public void putRow(String ds, String id, String pred, String value, boolean trim) {
        if (trim == true) {
            id = trimIt(id);
            value = trimIt(value);
        }
        putRow(nameConverter.makeTableName(ds, pred), id, value);
    }

    public void putRow(DataCell cell) {
        putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    public void putRow(DataCell cell, boolean trim) {
        putRow(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue(), trim);
    }

    public void putRowIfKeyValuePairIsAbsent(String tableName, String key, String value) {
        putRowIfKeyValuePairIsAbsent(tableName, key, value, false);
    }

    public void putRowIfKeyValuePairIsAbsent(DataCell cell) {
        putRowIfKeyValuePairIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    public void putRowIfKeyValuePairIsAbsent(String tableName, String key, String value, boolean trim) {
        if (trim == true) {
            key = trimIt(key);
            value = trimIt(value);
        }
        createTableIfAbsent(tableName);
        if (!hasRow(tableName, key, value)) {
            appendRow(tableName, key, value);
        }
    }

    public void putRowIfKeyValuePairIsAbsent(String dataset, String key, String pred, String value) {
        putRowIfKeyValuePairIsAbsent(dataset, key, pred, value, false);
    }

    public void putRowIfKeyValuePairIsAbsent(String dataset, String key, String pred, String value, boolean trim) {
        if (trim == true) {
            key = trimIt(key);
            value = trimIt(value);
        }
        putRowIfKeyValuePairIsAbsent(nameConverter.makeTableName(dataset, pred), key, value);
    }

    public void putRowIfKeyValuePairIsAbsent(DataCell cell, boolean trim) {
        putRowIfKeyValuePairIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue(), trim);
    }

    public void putRowIfKeyIsAbsent(String tableName, String key, String value) {
        putRowIfKeyIsAbsent(tableName, key, value, false);
    }

    public void putRowIfKeyIsAbsent(String tableName, String key, String value, boolean trim) {
        if (trim == true) {
            key = trimIt(key);
            value = trimIt(value);
        }

        createTableIfAbsent(tableName);
        if (!hasID(tableName, key)) {
            appendRow(tableName, key, value);
        }
    }

    public void putRowIfKeyIsAbsent(String dataset, String key, String pred, String value) {
        putRowIfKeyIsAbsent(dataset, key, pred, value, false);
    }

    public void putRowIfKeyIsAbsent(DataCell cell) {
        putRowIfKeyIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    public void putRowIfKeyIsAbsent(String dataset, String key, String pred, String value, boolean trim) {
        if (trim == true) {
            key = trimIt(key);
            value = trimIt(value);
        }
        putRowIfKeyIsAbsent(nameConverter.makeTableName(dataset, pred), key, value);
    }

    public void putRowIfKeyIsAbsent(DataCell cell, boolean trim) {
        putRowIfKeyValuePairIsAbsent(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue(), trim);
    }

    public void putRowWithReplacingValues(String tableName, String key, String value) {
        putRowWithReplacingValues(tableName, key, value, false);
    }

    public void putRowWithReplacingValues(String tableName, String key, String value, boolean trim) {
        //logger.debug("(overwriting)\"" + value + "\"");

        if (trim == true) {
            key = trimIt(key);
            value = trimIt(value);
        }

        //logger.debug("(overwriting)\"" + value + "\"");
        createTableIfAbsent(tableName);
        if (hasID(tableName, key)) {
            deleteID(tableName, key);
        }
        appendRow(tableName, key, value);

    }

    public void putRowWithReplacingValues(String dataset, String id, String pred, String value) {
        putRowWithReplacingValues(dataset, id, pred, value, false);
    }

    public void putRowWithReplacingValues(String dataset, String id, String pred, String value, boolean trim) {
        if (trim == true) {
            id = trimIt(id);
            value = trimIt(value);
        }
        putRowWithReplacingValues(nameConverter.makeTableName(dataset, pred), id, value);
    }

    public void putRowWithReplacingValues(DataCell cell) {
        putRowWithReplacingValues(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue());
    }

    public void putRowWithReplacingValues(DataCell cell, boolean trim) {
        putRowWithReplacingValues(cell.getDataSet(), cell.getID(), cell.getPredicate(), cell.getValue(), trim);
    }

	// ------
    public DCContainer setIterableTable(String ds, String pred) {
        closeIteratorIfExists();
        iterableTable = nameConverter.makeTableName(ds, pred);
        return this;
    }

    public DCContainer setIterableTable(String tableName) {
        closeIteratorIfExists();
        iterableTable = tableName;
        return this;
    }

    public void closeIteratorIfExists() {
        // nothing to do.
    }

    public Iterator<DataCell> getDataCellIterator(String ds, String pred) {
        this.setIterableTable(ds, pred);
        return this.iterator();
    }

    public ArrayList<String> getDataSetList() {
        return getDatasets();
    }

    public ArrayList<String> getDatasets() {
        ArrayList<String> result = new ArrayList<String>();
        TreeMap<String, String> uniqued = new TreeMap<String, String>();

        ArrayList<String> tableList = getTableList();
        for (String t : tableList) {
            // get a pair of internal names of a dataset and a predicate.
            ArrayList<String> internalNamePair = nameConverter.parseTableName(t);
            String datasetName = nameConverter.getOriginalName(internalNamePair.get(0));
            if (datasetName != null) {
                //System.err.println(internalNamePair + "\t" + t);
                if (datasetName.equals("INTERNAL_NAME_PREFIX")) // skip reserved tables.
                {
                    continue;
                }
                uniqued.put(datasetName, "true");
            }
        }

        Set<String> s = uniqued.keySet();
        for (String ds : s) {
            result.add(ds);
        }
        return result;
    }

    public ArrayList<String> getPredicateList(String ds) {
        return getPredicates(ds);
    }

    public ArrayList<String> getPredicates(String ds) {
        ArrayList<String> result = new ArrayList<String>();

        ArrayList<String> tableList = getTableList();
        for (String t : tableList) {
            if (t.equalsIgnoreCase("ORIGINAL_NAME__INTERNAL_NAME")
                    || t.equalsIgnoreCase("INTERNAL_NAME__ORIGINAL_NAME")) {
                continue;
            }

            // get a pair of internal names of a dataset and a predicate.
            ArrayList<String> internalNamePair = nameConverter.parseTableName(t);

            String datasetName = nameConverter.getOriginalName(internalNamePair.get(0).toUpperCase());
            String predicateName = nameConverter.getOriginalName(internalNamePair.get(1).toUpperCase());

            if (datasetName.equals(ds)) {
                result.add(predicateName);
            }
        }
        return result;

    }

    public String getInternalName(String originalName) {
        return nameConverter.getInternalName(originalName);
    }

    public String getOriginalName(String internalName) {
        return nameConverter.getOriginalName(internalName);
    }

    public int getPrefixMaxCount(String prefix) {
        return nameConverter.getCount(prefix);
    }

    public String getValue(String tableName, String id) {
        String result = null;
        ArrayList<String> values = getValueList(tableName, id);
        if (values != null && values.size() > 0) {
            result = values.get(0);
        }
        return result;
    }

    public String getValue(String ds, String id, String pred) {
        return getValue(nameConverter.makeTableName(ds, pred), id);
    }

    public NameConverter getNameConverter() {
        return nameConverter;
    }

    public boolean isValidTableName(String tableName) {
        return nameConverter.isValidTableName(tableName);
    }

	// -----------------------------------
    public Set<String> getIDSet(String ds, String pred) {
        Set<String> ids = new TreeSet<String>();
        for (DataCell cell : this.setIterableTable(ds, pred)) {
            ids.add(cell.getID());
        }
        return ids;
    }

    public Set<String> getIDSet(String ds, String[] preds) {
        Set<String> result = new TreeSet<String>();
        for (String pred : preds) {
            result.addAll(getIDSet(ds, pred));
        }
        return result;
    }

    public Set<String> getIDSet(String ds, ArrayList<String> preds) {
        Set<String> result = new TreeSet<String>();
        for (String pred : preds) {
            result.addAll(getIDSet(ds, pred));
        }
        return result;
    }

    public ArrayList<String> getIDList(String ds, String pred) {
        return SetUtil.toArrayList(getIDSet(ds, pred));
    }

    public ArrayList<String> getIDList(String ds, String[] preds) {
        return SetUtil.toArrayList(getIDSet(ds, preds));
    }

    public ArrayList<String> getIDList(String ds, ArrayList<String> preds) {
        return SetUtil.toArrayList(getIDSet(ds, preds));
    }

	//------------------------------------
    public String getDCText(String ds, String pred) {
        return getDCText(ds, new String[]{pred});
    }

    public String getDCText(String ds, String[] predList) {
		//String result = null;

        ArrayList<String> ids = this.getIDList(ds, predList);
        StringBuilder sb = new StringBuilder();
        for (String s : ids) {
            sb.append(getDCText(ds, s, predList) + "\n");
        }

        return sb.toString();
    }

    public String getDCText(String ds, ArrayList<String> predList) {
		//String result = null;

        ArrayList<String> ids = this.getIDList(ds, predList);
        StringBuilder sb = new StringBuilder();
        for (String s : ids) {
            sb.append(getDCText(ds, s, predList) + "\n");
        }

        return sb.toString();
    }

	//--
    public String getDCText(String ds, String id, String pred) {
        return getDCText(ds, id, new String[]{pred});
    }

    public String getDCText(String ds, String id, ArrayList<String> predList) {
        StringBuilder sb = new StringBuilder();
        sb.append("@ds: " + ds + "\n");
        sb.append("@id: " + id + "\n");

        for (String pred : predList) {
            ArrayList<String> valueList = this.getValueList(ds, id, pred);
            for (String value : valueList) {
                if (isSimpleValue(value)) {
                    sb.append("@@ " + pred + ": " + value + "\n");
                } else {
                    sb.append("@@ " + pred + "\n");
                    sb.append(trimLastLF(value) + "\n");
                    sb.append("@@ end\n");
                }
            }
        }
        return sb.toString();
    }

    public String getDCText(String ds, String id, String[] predList) {
        StringBuilder sb = new StringBuilder();
        sb.append("@ds: " + ds + "\n");
        sb.append("@id: " + id + "\n");

        for (String pred : predList) {
            ArrayList<String> valueList = this.getValueList(ds, id, pred);
            for (String value : valueList) {
                if (isSimpleValue(value)) {
                    sb.append("@@ " + pred + ": " + value + "\n");
                } else {
                    sb.append("@@ " + pred + "\n");
                    sb.append(trimLastLF(value) + "\n");
                    sb.append("@@ end\n");
                }
            }
        }
        return sb.toString();
    }

	//--
    public String getDCText(String ds, String[] ids, String pred) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, pred) + "\n");
        }
        return sb.toString();
    }

    public String getDCText(String ds, String[] ids, String[] preds) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, preds) + "\n");
        }
        return sb.toString();
    }

    public String getDCText(String ds, String[] ids, ArrayList<String> preds) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, preds) + "\n");
        }
        return sb.toString();
    }

    public String getDCText(String ds, ArrayList<String> ids, String pred) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, pred) + "\n");
        }
        return sb.toString();
    }

    public String getDCText(String ds, ArrayList<String> ids, String[] preds) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, preds) + "\n");
        }
        return sb.toString();
    }

    public String getDCText(String ds, ArrayList<String> ids, ArrayList<String> preds) {
        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(getDCText(ds, id, preds) + "\n");
        }
        return sb.toString();
    }

    public void readDCText(String dctext) {
        ArrayList<String> lines = StringUtil.splitByNewLine(dctext);
        readDCText(lines);
    }

    public void readDCText(ArrayList<String> lines) {
        for (String line : lines) {
            analyzeLine(line);
        }
    }

    public void readDCText(String dctext, String defaultDs) {
        this.setCurrentDataSet(defaultDs);
        ArrayList<String> lines = StringUtil.splitByNewLine(dctext);
        readDCText(lines);
    }

    public void readDCText(ArrayList<String> lines, String defaultDs) {
        this.setCurrentDataSet(defaultDs);
        for (String line : lines) {
            analyzeLine(line);
        }
    }

    public void analyzeLine(String line) {

        logger.trace("line: " + line);

        if (line == null) {
            logger.trace("line is null.");
            return;
        } else if (isDataSet(line)) {
            logger.trace("isDataSet");

            state = 0;

            ds = getDataSet(line);
            if (ds.equals("--") || ds.equals("---")) { // not specified.
                ds = getCurrentDataSet();
            }
            datacell = new DataCell();
            datacell.setDataSet(ds);
        } else if (isID(line)) {
            logger.trace("isID");

            state = 0;

            id = getID(line);
            rootDoc.add(id);
            if (id.equals("--") || id.equals("---")) { // not specified.
                id = generateID();
            }
        } else if (isPredicate(line)) {
            logger.trace("isPredicate");

            state = 0;

            ArrayList<String> kv = getPredAndValue(line);
            logger.debug(ds + ", " + id + ", " + kv.get(0) + ", " + kv.get(1));
            putRowIfKeyValuePairIsAbsent(ds, id, kv.get(0), kv.get(1));
        } else if (isTailOfBlock(line)) {
            logger.trace("isTailOfBlock");

            state = 0;
            logger.debug(ds + ", " + id + ", " + pred + ", " + sb.toString());
            putRowIfKeyValuePairIsAbsent(ds, id, pred, sb.toString());
        } else if (isHeadOfBlock(line)) {
            logger.trace("isHeadOfBlock");

            state = 1;
            pred = getPredicate(line);
            sb = new StringBuilder();

        } else if (state != 0) { // in a predicate block.
            logger.trace("state != 0 ... this line is in a predicate block.");

            sb.append(line + "\n");
        } else if (isCurrentDataSet(line)) {
            logger.trace("isCurrentDataSet");
            String cds = getDS(line);
            setCurrentDataSet(cds);
        } else {
            logger.trace("else!");
            // do nothing.
        }
    }

    public void makeCellOrderCell(DCContainer dbObj, String ds, String id, String pred) {
        DataCell dc = new DataCell();
        dc.setDataSet(ds);
        dc.setID("DUCKBILL_ROOT");
        dc.setPredicate("cell order");
        dc.setValue(ListUtil.join("\n", rootDoc));

        dbObj.putRowIfKeyValuePairIsAbsent(ds, dc.getID(), dc.getPredicate(), dc.getValue());
    }

    public void makeCellOrderCell(DCContainer dbObj, String ds, String id) {
        makeCellOrderCell(dbObj, ds, id, "cell order");
    }

    public void makeCellOrderCell(DCContainer dbObj, String ds) {
        makeCellOrderCell(dbObj, ds, "root_cell_order_cell", "cell order");
    }

    protected String getPredicate(String line) {
        String result = null;
        Matcher m = pHead.matcher(line);
        if (m.find()) {
            result = m.group(1);
        }
        return result;
    }

    protected ArrayList<String> getPredAndValue(String line) {
        ArrayList<String> result = null;
        Matcher m = pPredicate.matcher(line);
        if (m.find()) {
            result = new ArrayList<String>();
            result.add(m.group(1));
            result.add(m.group(2));
        }
        return result;
    }

    protected String generateID() {
        return UUID.randomUUID().toString();
    }

    protected String getID(String line) {
        String result = null;
        Matcher m = pID.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    protected String getCurrentDataSet() {
        return currentDataSet;
    }

    protected void setCurrentDataSet(String ds) {
        currentDataSet = ds;
    }

    /**
     *
     * @param line チェックする文字列
     * @return 検出されたデータセット名
     */
    private String getDataSet(String line) {
        String result = null;
        Matcher m = pDataSet.matcher(line);
        if (m.find()) {
            result = m.group(2);
        }
        return result;
    }

    /**
     * current data setを変える命令行から、data set名を取り出して返す.
     *
     * 以下のような行から、"your data set name"を取り出して返す。
     * {@code @$ ds: your data set name}
     *
     * current data setを変えると、DCTextの残りの部分でdata setが明示的に指定されていない場合に このcurrent
     * data setが使われる。
     *
     * @param line チェックする文字列
     * @return 検出されたデータセット名
     */
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
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isDataSet(String line) {
        boolean result = false;
        Matcher m = pDataSet.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isID(String line) {
        boolean result = false;
        Matcher m = pID.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isCurrentDataSet(String line) {
        boolean result = false;
        Matcher m = pDS.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isPredicate(String line) {

        boolean result = false;
        Matcher m = pPredicate.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isHeadOfBlock(String line) {

        boolean result = false;
        Matcher m = pHead.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    public boolean isTailOfBlock(String line) {

        boolean result = false;
        Matcher m = pTail.matcher(line);
        if (m.find()) {
            logger.debug(line);
            result = true;
        }
        return result;
    }

    // 改行を含まない80文字以下の文字列
    private boolean isSimpleValue(String str) {
        boolean result = true;
        if (str.length() > 80) {
            result = false;
        } else {
            for (int i = 0; i < str.length(); i++) {
                char ch = str.charAt(i);
                if (ch == '\n') {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    private String trimLastLF(String str) {
        String result = "";
        if (str.length() > 0) {
            char lastCh = str.charAt(str.length() - 1);
            if (lastCh == '\n' && str.length() > 1) {
                result = str.substring(0, str.length() - 2);
            } else {
                result = str;
            }
        }
        return result;
    }

}
