package com.github.oogasawa.datacell.h2;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.NameConverter;
import com.github.oogasawa.datacell.container.rdb.AbstractDCContainerRDB;
import com.github.oogasawa.datacell.container.rdb.PreparedStatementPool;

import org.apache.commons.configuration2.ex.ConfigurationException;



public class H2 extends AbstractDCContainerRDB {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    private int fetchSize = 1024;
    private int varcharMaxLength = 1000000000;

    public static void main(String[] args)
    {
        H2Factory facObj = new H2Factory();
        try {
            DCContainer dbObj = facObj.getInstance("test/testdb");
            dbObj.putRowIfKeyIsAbsent("taxonomy", "9606", "scientific name", "Homo sapiens");
            dbObj.putRowIfKeyIsAbsent("taxonomy", "9606", "common name", "human");

            dbObj.getTableList();
            dbObj.close();

        } catch (ConfigurationException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "main", e);
        }
    }

    public H2() {
        super();
        nameConverter = new NameConverter();
        nameConverter.setDatabase(this);
    }

    public void close() {
        closePool(insertPool);
        closePool(removePool);
        closePool(hasPool);
        closePool(hasKeyPool);

        try {
            if (connection != null) {
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "close", e);
        }

    }

    protected void closePool(PreparedStatementPool pool) {
        try {
            Collection<PreparedStatement> ps = pool.getAll();
            for (PreparedStatement p : ps) {
                p.close();
            }
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "closePool", e);
        }
    }

    public void dumpDB(String fname) {
        String sql = "SCRIPT TO '" + fname + "'";

        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "dumpDB", e);
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.h2.H2", "dumpDB", e);
            }
        }

    }

    @Override
    public void createTable(String tableName) {

        String sql1 = "create table " + tableName + " (" + "    id        VARCHAR(4000), " + "    content     VARCHAR("
                + varcharMaxLength + ") )";

        String sql2 = "create index " + tableName + "__id_index ON " + tableName + " (id)";

        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "createTable", e);
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.throwing("com.github.oogasawa.datacell.h2.H2", "createTable", e);
            }
        }

    }

    @Override
    public void deleteTable(String tableName) {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate("DROP TABLE " + tableName);
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "deleteTable", e);
        } finally {
            try {
                stmt.close();
            } catch (Exception e) {
                logger.throwing("com.github.oogasawa.datacell.h2.H2", "deleteTable", e);
            }
        }

    }

    @Override
    public void deleteTable(String dataset, String pred) {
        deleteTable(nameConverter.makeTableName(dataset, pred));
    }

    public ArrayList<String> getListOfAllTables() {
        ArrayList<String> tables = new ArrayList<String>();
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery("SHOW TABLES");
            while (resultSet.next()) {
                tables.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2", "deleteTable", e);
        } finally {
            try {
                stmt.close();
            } catch (Exception e) {
                logger.throwing("com.github.oogasawa.datacell.h2.H2", "deleteTable", e);
            }
        }
        return tables;
    }

    /**
     *
     *
     *
     */
    // @Override
    // public ArrayList<String> getTableList() {
    //
    // ArrayList<String> result = new ArrayList<String>();
    //
    // DatabaseMetaData dbmd;
    // try {
    // dbmd = connection.getMetaData();
    // ResultSet resultSet = dbmd.getTables(null, null, null, null);
    // while (resultSet.next()) {
    // String strTableName = resultSet.getString("TABLE_NAME");
    // String strTableType = resultSet.getString("TABLE_TYPE");
    // if (strTableType.equalsIgnoreCase("TABLE")) {
    // result.add(strTableName);
    // //System.err.println("TABLE_NAME is " + strTableName + "\t" + strTableType);
    // }
    // }
    //
    // } catch (SQLException e1) {
    // // TODO Auto-generated catch block
    // e1.printStackTrace();
    // }
    //
    //
    // return result;
    //
    // }
    public ArrayList<String> getValueList(String tableName, String key) {
        ArrayList<String> valueList = new ArrayList<String>();

        if (!hasTableInAllTables(tableName)) {
            return valueList;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement("SELECT value FROM " + tableName + " WHERE id=?");
            pstmt.setString(1, key);
            pstmt.setFetchSize(fetchSize);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                valueList.add(rs.getString(1));
            }
        } catch (Exception e) {
            logger.throwing("com.github.oogasawa.datacell.h2", "getValueList", e);
        } finally {
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    logger.throwing("com.github.oogasawa.datacell.h2", "getValueList", e);
                }
            }
        }

        return valueList;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;

    }

    public boolean hasKeyValue(String tableName, String key, String value) {
        boolean result = false;

        ArrayList<String> valueList = getValueList(tableName, key);
        for (String v : valueList) {
            if (v.equals(value)) {
                result = true;
                break;
            }
        }

        return result;
    }

}
