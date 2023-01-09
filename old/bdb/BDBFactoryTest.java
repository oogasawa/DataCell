package net.ogalab.datacell.bdb;

import static org.junit.Assert.*;

import net.ogalab.datacell.container.DCContainer;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BDBFactoryTest {

	Logger logger     = LoggerFactory.getLogger(BDBFactoryTest.class);
	
	@Test
	public void testCreateDB() {
		BDBFactory facObj = new BDBFactory();
		String dbName = "datacell_test";
		
		facObj.deleteDBIfExists(dbName);
		assertFalse(facObj == null);
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
			logger.info(dbObj.getValue("test data", "0001", "test value"));
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (dbObj != null)
				dbObj.close();
		}
		
		assertTrue(facObj.hasDB(dbName));
		facObj.deleteDB(dbName);
		assertFalse(facObj.hasDB(dbName));
	}

}
