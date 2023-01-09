package com.github.oogasawa.datacell.h2;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainerTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class H2Test extends DCContainerTest {

    private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");
    
    String dbName = "./datacell_h2_test";

    @Before
    public void setUp() throws Exception {
        H2Factory facObj = new H2Factory();
        logger.setLevel(Level.FINER);

        dbObj = facObj.getInstance(dbName);
        System.err.println("Message from H2Test.setUp() : default auth path = " + facObj.getAuthPath());

        dbObj.deleteAllDCTables();
    }

    @After
    public void tearDown() throws Exception {
        dbObj.close();
    }

    @Test
    public void testGetTableList2() {
        ArrayList<String> ts = dbObj.getTableList();
        for (String t : ts) {
            System.out.println(t);
        }
        //assertEquals(1, rowList.size());
    }

    /**
     * 全く同じkey-valueを２回appendRow()メソッドで入れた場合に、DB中に入るかどうかは実装依存である。
     *
     */
    @Override
    @Test
    public void testAppendRow2() {
        dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");

        ArrayList<DataCell> cellList = null;

        dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");

        cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
        assertEquals(1, cellList.size());
        assertEquals("117570", cellList.get(0).getID());
        assertEquals("Tereostomi", cellList.get(0).getValue());

        dbObj.appendRow("ncbi_taxonomy", "2759", "scientific name", "Eukaryota");
        cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
        assertEquals(2, cellList.size());

        // 全く同じkey-valueを入れたときに、2つDBに入る。
        dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");
        cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
        assertEquals(3, cellList.size());

    }

    @Override
    @Test
    public void testPutRow() {

        dbObj.putRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
        dbObj.putRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
        dbObj.putRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
        dbObj.putRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
        dbObj.putRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
        dbObj.putRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
        dbObj.putRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");

        ArrayList<String> result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
        assertEquals(2, result.size());
        assertEquals("Coturnix coturnix", result.get(0));
        assertEquals("Coturnix coturnix japonica", result.get(1));

        result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
        assertEquals(1, result.size());
        assertEquals("Carnivora", result.get(0));

        // When key, value strings have interruptive spaces, spaces are automatically trimmed.
        // Therefore, following data are treated as the same key, value.
        dbObj.putRow("ncbi_taxonomy", " 9091", "scientific name", " Coturnix coturnix");
        dbObj.putRow("ncbi_taxonomy", " 9091 ", "scientific name", " Coturnix coturnix japonica");
        dbObj.putRow("ncbi_taxonomy", "9774 ", "scientific name", "Sirenia ");
        dbObj.putRow("ncbi_taxonomy", " 9779", "scientific name", " Proboscidea ");
        dbObj.putRow("ncbi_taxonomy", " 9443   ", "scientific name", "Primates  ");
        dbObj.putRow("ncbi_taxonomy", " 9392", "scientific name", "     Scandenitia ");
        dbObj.putRow("ncbi_taxonomy", " 33554      ", "scientific name", "Carnivora     ");

        result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
        assertEquals(2, result.size());
        assertEquals("Coturnix coturnix", result.get(0));
        assertEquals("Coturnix coturnix japonica", result.get(1));

        result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
        assertEquals(1, result.size());
        assertEquals("Carnivora", result.get(0));

    }

    @Test
    public void testGetTableList() {
        dbObj.putRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
        dbObj.putRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
        dbObj.putRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
        dbObj.putRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");

        ArrayList<String> list = dbObj.getTableList();
        for (String t : list) {
            System.out.println(t);
        }

    }

    /*
	@Test
	public void testNamings() {
		dbObj.putRow("ncbi_taxonomy", "9091", "Date/Publication", "2012.01.01");
		dbObj.putRow("ncbi_taxonomy", "9092", "Date/Publication", "2011.11.12");		
		dbObj.putRow("ncbi_taxonomy", "9092", "日本語", "日本語の名前");	

		assertEquals("NONALNUM_0001", dbObj.getInternalName("Date/Publication"));
		assertEquals("NONALNUM_0002", dbObj.getInternalName("日本語"));
		assertEquals(2, dbObj.getPrefixMaxCount("NONALNUM"));
		
	}
     */

 /*
	@Test
	public void testCreateTableIfAbsent() {
		
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");
		
		assertEquals(6, dbObj.getTableList().size());

	}
	
	
	
	@Test
	public void testGetRowList() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		dbObj.appendRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.appendRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.appendRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.appendRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.appendRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");

		ArrayList<ArrayList<String>> rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(5, rowList.size());
		
	}
	
	
	
	@Test
	public void testAppendRow() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");
		assertEquals(6, dbObj.getTableList().size());

		
		ArrayList<ArrayList<String>> rowList   = null;
		
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");

		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(1, rowList.size());
		assertEquals("117570", rowList.get(0).get(0));
		assertEquals("Tereostomi", rowList.get(0).get(1));
		
		dbObj.appendRow("ncbi_taxonomy", "2759", "scientific name", "Eukaryota");
		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(2, rowList.size());
		
		
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");
		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(3, rowList.size());
		
	}
	
	
	@Test
	public void testGetRowListAsIterator() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		dbObj.appendRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.appendRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.appendRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.appendRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.appendRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<ArrayList<String>> rowList   = null;
		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(5, rowList.size());

		
		Iterator<ArrayList<String>> iter = dbObj.getRowListAsIterator("ncbi_taxonomy", "scientific name");
		String[] results = new String[5];
		int counter = 0;
		while (iter.hasNext()) {
			ArrayList<String> row = iter.next();
			counter++;
		}
		assertEquals(5, counter);
		
	}
	
	
	
	@Test
	public void testGetTableList() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");

		// Result of this test should be 6, because two additional tables are created internally.
		// (orignial_name__internal_name, internal_name__original_name)
		ArrayList<String> list = dbObj.getTableList();
		assertEquals(6, list.size()); 

	}
	  
	@Test
	public void testGetValueList() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		dbObj.appendRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.appendRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
		dbObj.appendRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.appendRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.appendRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.appendRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.appendRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<String> result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(2, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		assertEquals("Coturnix coturnix japonica", result.get(1));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));

	}
     */
 /*
	@Test
	public void testCreateTableString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteTableString() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetTableList() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetValueListStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testMySQL() {
		fail("Not yet implemented");
	}

	@Test
	public void testClosePool() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetFetchSize() {
		fail("Not yet implemented");
	}

	@Test
	public void testAppendRowStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteKeyStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteRowStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasKeyStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasKeyValueStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testIterator() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetConnection() {
		fail("Not yet implemented");
	}

	@Test
	public void testAppendRowStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateTableStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateTableIfAbsentString() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateTableIfAbsentStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteKeyStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteRowStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteTableStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteAllTables() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetRowList() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetRowListAsIterator() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetValueListStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasKeyStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasKeyValueStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasTableString() {
		fail("Not yet implemented");
	}

	@Test
	public void testHasTableStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowIfBothKeyAndValueAreAbsentStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowIfBothKeyAndValueAreAbsentStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowIfKeyIsAbsentStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowIfKeyIsAbsentStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowOverwritingStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testPutRowOverwritingStringStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetIterableTable() {
		fail("Not yet implemented");
	}
     */
}
