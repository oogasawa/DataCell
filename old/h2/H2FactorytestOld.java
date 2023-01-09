package com.github.oogasawa.datacell.h2;

import static org.junit.Assert.*;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.container.DCContainer;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

public class H2FactoryTest {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");

    @Before
    public void setUp() throws Exception {

        logger.setLevel(Level.FINER);
    }

    
    @Test
    /** Tests createDB() and deleteDB() methods of databases.
     * 
     */
    public void testCreateDB() {



        
        H2Factory facObj = new H2Factory();
        String dbName = "./datacell_h2_test"; // "This must be started with ./".

        facObj.deleteDBIfExists(dbName);
        assertFalse(facObj.hasDB(dbName));
        facObj.createDB(dbName);
        // The database is created when a new row is inserted.
        // Therefore, facObj.hasDB() will not be true 
        // immediately after invoking the createDB() method.
        assertFalse(facObj.hasDB(dbName));

        DCContainer dbObj = null;
        try {
            dbObj = facObj.getInstance(dbName);
            dbObj.putRow("test data", "0001", "test value", "あたい");
        } catch (ConfigurationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (dbObj != null) {
                dbObj.close();
            }
        }

        //assertTrue(facObj.hasDB(dbName));
        facObj.deleteDB(dbName);
        assertFalse(facObj.hasDB(dbName));
    }

}
