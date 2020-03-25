package net.ogalab.datacell.mem;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainerTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;

public class MemDBTest extends DCContainerTest {

	String     dbName = "columnar_test";
	
	@Before
	public void setUp() throws Exception {
		MemDBFactory facObj = new MemDBFactory();
		dbObj  = facObj.getInstance(dbName);
		
		dbObj.deleteAllDCTables();
	}

	@After
	public void tearDown() throws Exception {
		dbObj.close();
	}

	/** 全く同じkey-valueを２回appendRow()メソッドで入れた場合に、DB中に入るかどうかは実装依存である。
	 * 
	 */
	@Override
	@Test
	public void testAppendRow2() {
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		ArrayList<DataCell> cellList   = null;
		
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");

		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(1, cellList.size());
		assertEquals("117570", cellList.get(0).getID());
		assertEquals("Tereostomi", cellList.get(0).getValue());
		
		dbObj.appendRow("ncbi_taxonomy", "2759", "scientific name", "Eukaryota");
		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(2, cellList.size());
		
		// 全く同じkey-valueを入れたときに、2つDBに入らない。
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");
		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(2, cellList.size());
		
	}
	
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
	

	
	
	/*
	
	@Test
	public void testAppendRowStringStringString() {
		fail("Not yet implemented");
	}

	@Test
	public void testClose() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateTableString() {
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
	public void testMemDB() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetDbName() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetDbName() {
		fail("Not yet implemented");
	}
	*/

}
