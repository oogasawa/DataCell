package com.github.oogasawa.datacell.mem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.AbstractDCContainer;
import com.github.oogasawa.datacell.container.NameConverter;
import com.github.oogasawa.utility.types.collection.DuplicatedKeyUniqueValueHashMap;
import com.github.oogasawa.utility.types.collection.TreeSetUtil;

public class MemDB extends AbstractDCContainer {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell.mem");

    String dbName = null;

    HashMap<String, DuplicatedKeyUniqueValueHashMap<String, String>> mapOfTables = new HashMap<String, DuplicatedKeyUniqueValueHashMap<String, String>>();

    public MemDB() {
        super();
        nameConverter = new NameConverter();
        nameConverter.setDatabase(this);
    }

    public MemDB(String dbName) {
        super();
        this.dbName = dbName;

        nameConverter = new NameConverter();
        nameConverter.setDatabase(this);
    }

    public void appendRow(String tableName, String id, String value) {
        // log.debug(id + "\t" + value);
        if (value == null)
            value = "\\N";
        if (id == null)
            id = "\\N";
        mapOfTables.get(tableName).put(id, value);
    }

    public void close() {
        // nothing to do.
    }

    public void createTable(String tableName) {
        DuplicatedKeyUniqueValueHashMap<String, String> map = new DuplicatedKeyUniqueValueHashMap<String, String>();
        mapOfTables.put(tableName, map);
    }

    public void deleteID(String tableName, String id) {
        DuplicatedKeyUniqueValueHashMap<String, String> map = mapOfTables.get(tableName);
        if (map != null) {
            map.remove(id);
        }
    }

    public void deleteRow(String tableName, String id, String value) {
        DuplicatedKeyUniqueValueHashMap<String, String> map = mapOfTables.get(tableName);
        if (map != null) {
            if (map.containsKey(id)) {
                ArrayList<String> valueList = map.getValueList(id);
                for (String v : valueList) {
                    if (v.equals(value)) {
                        valueList.remove(v);
                    }
                }
            }
        }
    }

    public void deleteTable(String tableName) {
        mapOfTables.remove(tableName);
    }

    public ArrayList<String> getListOfAllTables() {
        ArrayList<String> result = new ArrayList<String>();
        Set<String> ts = mapOfTables.keySet();

        for (String s : ts) {
            result.add(s);
        }

        return result;
    }

    public ArrayList<String> getValueList(String tableName, String id) {

        // logger.debug(tableName);

        ArrayList<String> result = new ArrayList<String>();
        DuplicatedKeyUniqueValueHashMap<String, String> m = mapOfTables.get(tableName);
        if (m != null && m.containsKey(id)) {
            result = TreeSetUtil.toArrayList(mapOfTables.get(tableName).get(id));
        }

        return result;
    }

    public Iterator<DataCell> iterator() {
        NameConverter nc = this.getNameConverter();
        ArrayList<String> n = nc.parseTableName(iterableTable);
        return new Iter(mapOfTables.get(iterableTable), nc.getOriginalName(n.get(0)), nc.getOriginalName(n.get(1)));
    }

    public boolean hasID(String tableName, String id) {
        return mapOfTables.get(tableName).containsKey(id);
    }

    public boolean hasRow(String tableName, String id, String value) {
        boolean result = false;

        DuplicatedKeyUniqueValueHashMap<String, String> map = mapOfTables.get(tableName);
        if (map != null) {
            if (map.containsKey(id)) {
                ArrayList<String> valueList = map.getValueList(id);
                for (String v : valueList) {
                    if (v.equals(value)) {
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    private class Iter implements Iterator<DataCell> {

        DuplicatedKeyUniqueValueHashMap<String, String> table = null;
        Set<String> keySet = null;
        Iterator<String> keyIter = null;
        Iterator<String> valueIter = null;

        final int ITERATE_OVER_KEY = 0;
        final int ITERATE_OVER_VALUE = 1;

        int state = ITERATE_OVER_KEY;

        String currentKey = null;
        String currentValue = null;
        ArrayList<String> currentValueList = null;

        String dataSet = null;
        String predicate = null;

        @SuppressWarnings("unchecked")
        public Iter(DuplicatedKeyUniqueValueHashMap<String, String> table, String ds, String pred) {
            this.table = table;
            keySet = (Set<String>) this.table.keySet();

            keyIter = keySet.iterator();
            dataSet = ds;
            predicate = pred;
        }

        
        public boolean hasNext() {
            boolean result = false;
            if (state == ITERATE_OVER_KEY) {
                result = keyIter.hasNext();
            } else if (state == ITERATE_OVER_VALUE) {
                result = valueIter.hasNext();
                if (result == false) {
                    result = keyIter.hasNext();
                }
            }

            return result;
        }

        
        public DataCell next() {
            DataCell result = new DataCell();
            if (state == ITERATE_OVER_KEY) {
                currentKey = keyIter.next();

                currentValueList = table.getValueList(currentKey);
                valueIter = currentValueList.iterator();
                currentValue = valueIter.next();

                result.setDataSet(dataSet);
                result.setID(currentKey);
                result.setPredicate(predicate);
                result.setValue(currentValue);

                state = ITERATE_OVER_VALUE;
            } else if (state == ITERATE_OVER_VALUE) {
                if (valueIter.hasNext()) {
                    currentValue = valueIter.next();

                    result.setDataSet(dataSet);
                    result.setID(currentKey);
                    result.setPredicate(predicate);
                    result.setValue(currentValue);
                } else {
                    state = ITERATE_OVER_KEY;
                    result = next();
                }
            }

            return result;

        }

        public void remove() {
            if (currentValueList.size() == 1) {
                keyIter.remove();
            } else if (currentValueList.size() > 1) {
                valueIter.remove();
            }
        }


        public void forEachRemaining(Consumer<? super DataCell> action) {
            // TODO Auto-generated method stub
        }

    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

}
