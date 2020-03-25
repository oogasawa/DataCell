package net.ogalab.datacell.mem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MemDBFactoryTest {

	@Test
	public void testCreateDB() {
		MemDBFactory facObj = new MemDBFactory();
		String dbName = "datacell_test";
		
		facObj.deleteDBIfExists(dbName);
		assertFalse(facObj.hasDB(dbName));
		facObj.createDB(dbName);
		assertTrue(facObj.hasDB(dbName));
		facObj.deleteDB(dbName);
		assertFalse(facObj.hasDB(dbName));
	}

}
