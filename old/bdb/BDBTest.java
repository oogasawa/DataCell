package net.ogalab.datacell.bdb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;

import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainerFactory;
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

public class BDBTest extends DCContainerTest {

	String     dbName = "berkeleydbtest.db";
	
	@Before
	public void setUp() throws Exception {
		DCContainerFactory facObj = new BDBFactory();
		facObj.createDBIfAbsent(dbName);
		dbObj  = facObj.getInstance(dbName);
		
		dbObj.deleteAllDCTables();
	}

	@After
	public void tearDown() throws Exception {
		if (dbObj != null)
			dbObj.close();
	}

/*
	@Test
	public void testCreateTableIfAbsent() {
		
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");
		
		assertEquals(6, dbObj.getTableList().size());

	}
*/	
	
	
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
	public void testGetRowListAsIterator() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		dbObj.appendRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.appendRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.appendRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.appendRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.appendRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<DataCell> cellList   = null;
		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(5, cellList.size());

		
		Iterator<DataCell> iter = dbObj.getDataCellIterator("ncbi_taxonomy", "scientific name");
		//String[] results = new String[5];
		int counter = 0;
		while (iter.hasNext()) {
			@SuppressWarnings("unused")
			DataCell row = iter.next();
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
		assertEquals(4, list.size()); 

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
    


/*
	

	public void deleteKey(String dataset, String key, String predicate);
	
	public void deleteKey(String tableName, String key);

	public void deleteRow(String tableName, String key, String value);
	
	public void deleteRow(String dataset, String key, String predicate, String value);
	
	public void deleteTable(String tableName);
	
	public void deleteTable(String dataset, String pred);
	
	public void deleteAllTables();
	
	


  
		

	public boolean hasKey(String dataset, String key, String pred);
	
	public boolean hasKey(String tableName, String key);
	
	public boolean hasKeyValue(String dataset, String key, String pred, String value);
	
	public boolean hasKeyValue(String tableName, String key, String value);
	
	public boolean hasTable(String tableName);
	
	public boolean hasTable(String dataset, String pred);

	public Iterator<ArrayList<String>> iterator();
	

	public void putRow(String tableName, String key, String value);
	

	public void putRow(String dataset, String key, String predicate, String value);
	
	public void putRowIfBothKeyAndValueAreAbsent(String tableName, String key, String value);
	
	public void putRowIfBothKeyAndValueAreAbsent(String dataset, String key, String predicate, String value);
	
	public void putRowIfKeyIsAbsent(String tableName, String key, String value); // keyが一致したら何もしない
	
	public void putRowIfKeyIsAbsent(String dataset, String key, String pred, String value); // keyが一致したら何もしない
	
	public void putRowOverwriting(String tableName, String key, String value); // keyが一致したらvalueを上書き
	


	
	//public String getDataset(String tableName);
	
	//public String getPredicate(String tableName);
	
	//public String getTableName(String dataset, String predicate);
	
	//public String getReversedTableName(String tableName);
	
	//public void printTable(String tableName);
	
	//public void saveTable(String tableName, String filename);
	

	public void putRowOverwriting(String dataset, String key, String pred, String value); // keyが一致したらvalueを上書き
	

	public ColumnarDB setIterableTable(String dataset, String pred);

*/
}
