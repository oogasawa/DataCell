package com.github.oogasawa.datacell.h2;

import java.io.File;
import java.io.FileFilter;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.container.AbstractDCContainerFactory;
import com.github.oogasawa.datacell.container.DCContainer;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.filefilter.WildcardFileFilter;

public class H2Factory extends AbstractDCContainerFactory {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    public H2Factory() {
    }

    /**
     * H2 databaseのColumnarDBインスタンスを作る.
     *
     *
     * @param dbName
     * @return
     * @throws ConfigurationException
     */
    @Override
    public DCContainer getInstance(String dbName) throws ConfigurationException {

        H2 dbObj = new H2();
        Connection connection = null;

        // ArrayList<String> info = parseDbName(dbName); // info = {dbpath, name}
        // String dirName = info.get(0);
        // String dbName = info.get(1);
        // setDbDirectory(info.get(0));
        try {
            Class.forName("org.h2.Driver").getDeclaredConstructor().newInstance();
            // Make a connection.
            connection = DriverManager.getConnection("jdbc:h2:" + dbName, "sa", "p");

            dbObj.setConnection(connection);
            // dbObj.setFetchSize(fetchSize);

        } catch (SQLException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2Factory", "getInstance", e);
            logger.log(Level.WARNING, e.getSQLState(), e);
            // System.err.println(e.getSQLState());
            // e.printStackTrace();
        } catch (ClassNotFoundException
                 | InstantiationException
                 | IllegalAccessException
                 | IllegalArgumentException
                 | InvocationTargetException
                 | NoSuchMethodException
                 | SecurityException e) {
            logger.throwing("com.github.oogasawa.datacell.h2.H2Factory", "getInstance", e);
        }


        return dbObj;
    }

    @Override
    public void createDB(String dbName) {
        // By default, if the database specified in the URL does not yet exist,
        // a new (empty) database is created automatically.
        // The user that created the database automatically becomes the administrator of
        // this database.

        // Therefore ..., nothing to do.

    }

    @Override
    public void deleteDB(String dbName) {
        // To delete database, delete the database file by using an OS command.
        File dir = new File(".");
        FileFilter fileFilter = new WildcardFileFilter(dbName + "*.db");
        File[] files = dir.listFiles(fileFilter);
        for (File f : files) {
            f.delete();
        }
    }

    public boolean hasDB(String dbName) {
        File dir = new File(".");
        FileFilter fileFilter = new WildcardFileFilter(dbName + "*.db");
        File[] files = dir.listFiles(fileFilter);
        if (files == null || files.length == 0) {
            return false;
        } else {
            return true;
        }
    }

    /*
     * public ArrayList<String> parseDbName(String dbName) { ArrayList<String>
     * result = new ArrayList<String>();
     * 
     * Pattern p = Pattern.compile("(.+)\\/(\\w+)$"); Matcher m = p.matcher(dbName);
     * if (m.find()) { result.add(m.group(1)); result.add(m.group(2)); }
     * 
     * return result; }
     */
    /*
     * public void setDbDirectory(String dbDir) {
     * 
     * if (dbDir.startsWith("/")) { // absolute path
     * System.setProperty("derby.system.home", dbDir); } else { // relative path
     * from the current working directory. System.setProperty("derby.system.home",
     * System.getProperty("user.dir") + "/" + dbDir); } }
     */
}
