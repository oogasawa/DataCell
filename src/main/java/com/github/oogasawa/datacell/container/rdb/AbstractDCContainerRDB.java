package com.github.oogasawa.datacell.container.rdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.AbstractDCContainer;
import com.github.oogasawa.datacell.container.NameConverter;

abstract public class AbstractDCContainerRDB extends AbstractDCContainer {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    // データをinsertするためのPreparedStatementのプール
    protected PreparedStatementPool insertPool = new PreparedStatementPool();
    protected PreparedStatementPool removePool = new PreparedStatementPool();

    // データの存在を検査するためのPreparedStatementのプール
    protected PreparedStatementPool hasPool = new PreparedStatementPool();
    protected PreparedStatementPool hasKeyPool = new PreparedStatementPool();

    protected String dbName = null;
    protected Connection connection = null;

    protected Statement commit = null;

    protected Iterator<ArrayList<String>> iter = null;

    public void appendRow(String tableName, String key, String value) {
        PreparedStatement pstmt = null;
        // logger.debug("(appendRow: insert): \"" + value + "\"");

        try {
            pstmt = insertPool.get(tableName);
            if (pstmt == null) {
                String sql = "insert into " + tableName + " VALUES ( ?, ? )";
                pstmt = connection.prepareStatement(sql);
                insertPool.put(tableName, pstmt);
            }

            pstmt.setString(1, key);
            pstmt.setString(2, value);

            pstmt.executeUpdate();
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "appenRow", e);
            // logger.warn("appendRow: tableName: " + tableName);
            // logger.warn("appendRow: key: " + key);
            // logger.warn("appendRow: value: " + value);
            if (value.length() > 100) {
                logger.warning("appendRow: value: " + value.substring(0, 20) + " ...");
                logger.warning("appendRow: value length: " + value.length());
            } else {
                logger.warning("appendRow: value: " + value);
            }
            ArrayList<String> name = nameConverter.parseTableName(tableName);
            logger.warning("appendRow: data set: " + name.get(0));
            logger.warning("appendRow: predicate: " + name.get(1));

        }

    }

    public boolean getAutoCommit() {
        boolean result = false;
        try {
            result = connection.getAutoCommit();
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "getAutoCommit", e);
        }
        return result;
    }

    public void setAutoCommit(boolean ac) {
        try {
            connection.setAutoCommit(ac);
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "setAutoCommit", e);
        }
    }

    public void commit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "commit", e);
        }
    }

    public void deleteID(String tableName, String key) {
        PreparedStatement pstmt = null;

        try {
            pstmt = removePool.get(tableName);
            if (pstmt == null) {
                String sql = "delete from " + tableName + " where id=?";
                pstmt = connection.prepareStatement(sql);
                removePool.put(tableName, pstmt);
            }

            pstmt.setString(1, key);
            pstmt.executeUpdate();
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "deleteID", e);
            logger.warning("deleteKey: tableName: " + tableName);
            logger.warning("deleteKey: key: " + key);
            ArrayList<String> name = nameConverter.parseTableName(tableName);
            logger.warning("deleteKey: data set: " + name.get(0));
            logger.warning("deleteKey: predicate: " + name.get(1));
        }
    }

    public void deleteRow(String tableName, String key, String value) {
        PreparedStatement pstmt = null;

        try {
            pstmt = removePool.get(tableName);
            if (pstmt == null) {
                String sql = "delete from " + tableName + " where id=? AND value=?";
                pstmt = connection.prepareStatement(sql);
                removePool.put(tableName, pstmt);
            }

            pstmt.setString(1, key);
            pstmt.setString(2, value);
            pstmt.executeUpdate();
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "deleteRow", e);
            logger.warning("deleteRow: tableName: " + tableName);
            logger.warning("deleteRow: key: " + key);
            logger.warning("deleteRow: value: " + value);
            ArrayList<String> name = nameConverter.parseTableName(tableName);
            logger.warning("deleteRow: data set: " + name.get(0));
            logger.warning("deleteRow: predicate: " + name.get(1));
        }
    }

    /* abstract rdb */
    public boolean hasID(String tableName, String key) {

        boolean result = false;

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            this.createTableIfAbsent(tableName);

            pstmt = hasKeyPool.get(tableName);
            if (pstmt == null) {
                String sql = "select * from " + tableName + " where id=?";
                pstmt = connection.prepareStatement(sql);
                hasKeyPool.put(tableName, pstmt);
            }

            pstmt.setString(1, key);
            rs = pstmt.executeQuery();
            result = rs.next();
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "hasID", e);
            logger.warning("hasKey: tableName: " + tableName);
            logger.warning("hasKey: key: " + key);
            ArrayList<String> name = nameConverter.parseTableName(tableName);
            logger.warning("hasKey: data set: " + name.get(0));
            logger.warning("hasKey: predicate: " + name.get(1));
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "hasID", e);
                logger.warning("hasKey: tableName: " + tableName);
                logger.warning("hasKey: key: " + key);
                ArrayList<String> name = nameConverter.parseTableName(tableName);
                logger.warning("hasKey: data set: " + name.get(0));
                logger.warning("hasKey: predicate: " + name.get(1));
            }
        }
        return result;
    }

    /* abstract rdb */
    public boolean hasRow(String tableName, String key, String value) {
        boolean result = false;

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            pstmt = hasPool.get(tableName);
            if (pstmt == null) {
                String sql = "select * from " + tableName + " where id=? AND value=?";
                // System.err.println(sql);
                pstmt = connection.prepareStatement(sql);
                hasPool.put(tableName, pstmt);
            }

            pstmt.setString(1, key);
            pstmt.setString(2, value);
            rs = pstmt.executeQuery();
            result = rs.next();
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "hasRow", e);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB", "hasRow", e);
            }
        }
        return result;
    }

    /*
     * public Iterator<ArrayList<String>> getNewIterator() { return new
     * Itr(iterableTable); }
     */
    public Iterator<DataCell> iterator() {
        NameConverter nc = this.getNameConverter();
        // a pair of internal names.
        ArrayList<String> n = nc.parseTableName(iterableTable);
        this.createTableIfAbsent(iterableTable);
        return new Itr(iterableTable, nc.getOriginalName(n.get(0)), nc.getOriginalName(n.get(1)));
    }

    private class Itr implements Iterator<DataCell> {

        int fetchSize = 32;
        Statement stmt = null;
        ResultSet rs = null;

        // hasNext(), next() issue.
        // http://stackoverflow.com/questions/1870022/java-resultset-hasnext
        private boolean didNext = false;
        private boolean hasNext = false;

        private String dataSet = null;
        private String predicate = null;

        public Itr(String tableName, String ds, String pred) {
            dataSet = ds;
            predicate = pred;

            String sql = "SELECT id, value FROM " + tableName;
            try {
                stmt = connection.createStatement();
                stmt.setFetchSize(fetchSize);
                rs = stmt.executeQuery(sql);
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "constructor", e);
                logger.warning("Itr constructor: tableName: " + tableName);
                ArrayList<String> name = nameConverter.parseTableName(tableName);
                logger.warning("Itr constructor: data set: " + name.get(0));
                logger.warning("Itr constructor: predicate: " + name.get(1));
            }
        }

        public boolean hasNext() {
            try {
                if (!didNext) {
                    hasNext = rs.next();
                    didNext = true;
                }
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "hasNext", e);
            }
            return hasNext;
        }

        /*
         * http://stackoverflow.com/questions/1870022/java-resultset-hasnext
         * 
         * public Object next(){ if (!didNext) { entities.next(); } didNext = false;
         * return new Entity(entities.getString...etc....) }
         */
        public DataCell next() {
            DataCell result = null;
            try {
                if (!didNext) {
                    rs.next();
                }
                didNext = false;

                // System.err.println("now I'm in next() method : " + rs.getString("id") + ", "
                // + rs.getString("value"));
                result = new DataCell();
                result.setDataSet(dataSet);
                result.setID(rs.getString("id"));
                result.setPredicate(predicate);
                result.setValue(rs.getString("value"));
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "next", e);
            }
            // System.err.println("now I'm at the end of next() method : " + result.get(0) +
            // ", " + result.get(1));
            return result;
        }

        public void remove() {
            try {
                rs.deleteRow();
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "remove", e);
            }

        }

        public void dispose() {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "dispose", e);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "dispose", e);
                }
            }

        }

        @Override
        protected void finalize() {
            try {
                super.finalize();
            } catch (Throwable e) {
                logger.throwing("com.github.oogasawa.datacell.container.rdb.Itr", "finalize", e);
            } finally {
                dispose();
            }
        }

    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

}
