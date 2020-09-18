package net.ogalab.datacell.container;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;

import net.ogalab.datacell.DataCell;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class DCContainerTest {
	
	Logger log = LoggerFactory.getLogger(DCContainerTest.class);
	
	protected DCContainer dbObj  = null;

	@Before
	abstract public void setUp() throws Exception;

	@After
	abstract public void tearDown() throws Exception;


	/*
	 * テスト成功せず！！後日！！！
	 
	@Test
	public void testCreateTableOverwriting() {
		dbObj.createTableIfAbsent("ncbi_taxonomy2", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy2", "common name");
		dbObj.createTableIfAbsent("species20002", "scientific name");
		dbObj.createTableIfAbsent("species20002", "common name");
		
		assertEquals(6, dbObj.getTableList().size());

		ArrayList<ArrayList<String>> rowList   = null;
		dbObj.appendRow("ncbi_taxonomy2", "117570", "scientific name", "Tereostomi");
		rowList = dbObj.getRowList("ncbi_taxonomy2", "scientific name");
		assertEquals(1, rowList.size());
		assertEquals("117570", rowList.get(0).get(0));
		assertEquals("Tereostomi", rowList.get(0).get(1));
		
		dbObj.createTableOverwriting("ncbi_taxonomy2", "scientific name");
		rowList = dbObj.getRowList("ncbi_taxonomy2", "scientific name");
		assertEquals(0, rowList.size());
		
		
	}
*/
	
	@Test
	public void testGetDsPredPairs() {
		
		dbObj.putRow("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.putRow("ncbi_taxonomy", "9091", "English", "quail");
		dbObj.putRow("big_table", "100", "", "xxx");
		
		ArrayList<String>     tableList = dbObj.getTableList();
		NameConverter nc = dbObj.getNameConverter();		
		for (String tableName : tableList) {
			System.err.println("tgdpp: 0:" + tableName);
			
			ArrayList<String> pair0 = nc.parseTableName(tableName);
			System.err.println("tgdpp: 1:" + pair0.get(0) + "\t" + pair0.get(1));
			ArrayList<String> pair  = new ArrayList<String>();
			System.err.println("tgdpp: 2:" + nc.getOriginalName(pair0.get(0)));
			pair.add(nc.getOriginalName(pair0.get(0)));
			System.err.println("tgdpp: 3:" + nc.getOriginalName(pair0.get(1)));
			pair.add(nc.getOriginalName(pair0.get(1)));
		}
		ArrayList<ArrayList<String>> result = dbObj.getDsPredPairs();
		
		for (ArrayList<String> pair : result) {
			System.err.println("tgdpp: 4:" + pair.get(0) + "\t" + pair.get(1));
		}

//		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
//		assertEquals(1, result.size());
//		assertEquals("Carnivora", result.get(0));
		
	}
	
	
	@Test
	public void testCreateTableIfAbsent() {
		
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");
		
		assertEquals(4, dbObj.getTableList().size());

	}
	
	
	
	@Test
	public void testGetDataCellList() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		
		dbObj.appendRow("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.appendRow("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.appendRow("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.appendRow("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.appendRow("ncbi_taxonomy", "33554", "scientific name", "Carnivora");

		ArrayList<DataCell> cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(5, cellList.size());
		
	}
	
	
	
	@Test
	public void testAppendRow() {

		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		dbObj.createTableIfAbsent("ncbi_taxonomy", "common name");
		dbObj.createTableIfAbsent("species2000", "scientific name");
		dbObj.createTableIfAbsent("species2000", "common name");
		assertEquals(4, dbObj.getTableList().size());

		
		ArrayList<DataCell> cellList   = null;
		
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");

		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(1, cellList.size());
		assertEquals("117570", cellList.get(0).getID());
		assertEquals("Tereostomi", cellList.get(0).getValue());
		
		dbObj.appendRow("ncbi_taxonomy", "2759", "scientific name", "Eukaryota");
		cellList = dbObj.getDataCellList("ncbi_taxonomy", "scientific name");
		assertEquals(2, cellList.size());
	}
		
	/** 全く同じkey-valueを２回appendRow()メソッドで入れた場合に、DB中に入るかどうかは実装依存である。
	 * 
	 */
	@Test
	abstract public void testAppendRow2();
	/*
	{
		dbObj.createTableIfAbsent("ncbi_taxonomy", "scientific name");
		ArrayList<ArrayList<String>> rowList   = null;
		
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");

		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(1, rowList.size());
		assertEquals("117570", rowList.get(0).get(0));
		assertEquals("Tereostomi", rowList.get(0).get(1));
		
		dbObj.appendRow("ncbi_taxonomy", "2759", "scientific name", "Eukaryota");
		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(2, rowList.size());
		
		// 全く同じkey-valueを入れたときに、それが２つDB中に入るかどうかは ... 実装依存？入るべき？？
		// 後で同じものの個数を数えたいときには効いてくることですが。
		dbObj.appendRow("ncbi_taxonomy", "117570", "scientific name", "Tereostomi");
		rowList = dbObj.getRowList("ncbi_taxonomy", "scientific name");
		assertEquals(3, rowList.size());
		
	}
	*/
	
	
	@Test
	public void testGetDataCellListAsIterator() {

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
	
	@Test
	abstract public void testPutRow();
	/*
	{
		
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
	*/
	
	

	@Test
	public void testPutRowIfBothKeyAndValueAreAbsent() {
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<String> result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(2, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		assertEquals("Coturnix coturnix japonica", result.get(1));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));
		
		// When key, value strings have interruptive spaces, spaces are automatically trimmed.
		// Therefore, following data are treated as the same key, value.
		
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 9091", "scientific name", " Coturnix coturnix");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 9091 ", "scientific name", " Coturnix coturnix japonica");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9774 ", "scientific name", "Sirenia ");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 9779", "scientific name", " Proboscidea ");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 9443   ", "scientific name", "Primates  ");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 9392", "scientific name", "     Scandenitia ");
		dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", " 33554      ", "scientific name", "Carnivora     ");
		
		result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(2, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		assertEquals("Coturnix coturnix japonica", result.get(1));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));
	}
	
	@Test
	public void testPutRowIfKeyIsAbsent() {
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<String> result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));
		
		// When key, value strings have interruptive spaces, spaces are automatically trimmed.
		// Therefore, following data are treated as the same key, value.
		
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 9091", "scientific name", " Coturnix coturnix");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 9091 ", "scientific name", " Coturnix coturnix japonica");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", "9774 ", "scientific name", "Sirenia ");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 9779", "scientific name", " Proboscidea ");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 9443   ", "scientific name", "Primates  ");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 9392", "scientific name", "     Scandenitia ");
		dbObj.putRowIfKeyIsAbsent("ncbi_taxonomy", " 33554      ", "scientific name", "Carnivora     ");
		
		result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));
	}
	
	

	@Test
	public void testPutRowOverwriting() {
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9779", "scientific name", "Proboscidea");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9443", "scientific name", "Primates");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9392", "scientific name", "Scandenitia");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "33554", "scientific name", "Carnivora");
		
		ArrayList<String> result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Coturnix coturnix japonica", result.get(0));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora", result.get(0));
		
		// When key, value strings have interruptive spaces, spaces are automatically trimmed.
		// Therefore, following data are treated as the same key, value.
		
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9774", "scientific name", "Sirenia ");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9779", "scientific name", " Proboscidea ");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9443", "scientific name", "Primates  ");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "9392", "scientific name", "     Scandenitia ");
		dbObj.putRowByOverwriting("ncbi_taxonomy", "33554", "scientific name", "Carnivora     ");
		
		result = dbObj.getValueList("ncbi_taxonomy", "9091", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Coturnix coturnix", result.get(0));
		
		result = dbObj.getValueList("ncbi_taxonomy", "33554", "scientific name");
		assertEquals(1, result.size());
		assertEquals("Carnivora     ", result.get(0));
	}
    
	@Test
	public void testIterator() {
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "uzura");		
		dbObj.putRow("ncbi_taxonomy", "9774", "name", "Sirenia");
		dbObj.putRow("ncbi_taxonomy", "9779", "name", "Proboscidea");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix japonica");

		dbObj.setIterableTable("ncbi_taxonomy", "name");
		
		// iterator()は毎回新しいiterオブジェクトを作って返すのか？
		// iterator()は一つだけオブジェクトを持ち、それを返すのか？　< -- こちら。
		Iterator<DataCell> iter = dbObj.iterator();
	
		// データが、入れられた順番に取り出されることは保証されていない。
		int count9091 = 0;
		int count9774 = 0;
		int count9779 = 0;
		int countAll = 0;
		while (iter.hasNext()) {
			DataCell row = iter.next();
			if (row.getID().equals("9091"))
				count9091++;
			if (row.getID().equals("9774"))
				count9774++;
			if (row.getID().equals("9779"))
				count9779++;

			countAll++;
		}
		assertEquals(3, count9091);
		assertEquals(1, count9774);
		assertEquals(1, count9779);
		assertEquals(5, countAll);

	}
	
	@Test
	public void testGetDatasets() {
		
		ArrayList<String> ds = null;
		
		ds = dbObj.getDatasets();
		assertTrue(ds.size() == 0);
		
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "uzura");		
		dbObj.putRow("ncbi_taxonomy", "9774", "name", "Sirenia");
		dbObj.putRow("ncbi_taxonomy", "9779", "name", "Proboscidea");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix japonica");

		ds = dbObj.getDatasets();
		assertTrue(ds.size() == 1);
	
		dbObj.putRow("tarball", "aaa-10.0.tar.gz", "Date/Publication", "Coturnix coturnix");
		ds = dbObj.getDatasets();
		assertTrue(ds.size() == 2);


		
	}
	
	@Test
	public void testGetPredicates() {
		
		ArrayList<String> ds = null;
		
		ds = dbObj.getDatasets();
		assertTrue(ds.size() == 0);
		
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "uzura");		
		dbObj.putRow("ncbi_taxonomy", "9774", "name", "Sirenia");
		dbObj.putRow("ncbi_taxonomy", "9779", "name", "Proboscidea");
		dbObj.putRow("ncbi_taxonomy", "9091", "name", "Coturnix coturnix japonica");

		ds = dbObj.getDatasets();
		//System.err.println(ds);
		assertTrue(ds.size()==1);
	
		dbObj.putRow("tarball", "aaa-10.0.tar.gz", "Date/Publication", "Coturnix coturnix");
		ds = dbObj.getDatasets();
		//System.err.println(ds);
		assertTrue(ds.size()==2);

		ArrayList<String> preds = dbObj.getPredicates("ncbi_taxonomy");
		assertTrue(preds.size() == 1);

		preds = dbObj.getPredicates("tarball");
		assertTrue(preds.size() == 1);
		
	}


	/*
	@Test
	public void testCreateTableString() {
		
		dbObj.putRow("ncbi_taxonomy", "9091", "Date/Publication", "2012.01.01");
		dbObj.putRow("ncbi_taxonomy", "9092", "Date/Publication", "2011.11.12");		
		dbObj.putRow("ncbi_taxonomy", "9092", "日本語", "日本語の名前");	
		
		assertEquals("NONALNUM_0001", dbObj.getInternalName("Date/Publication"));
		assertEquals("NONALNUM_0002", dbObj.getInternalName("日本語"));

		ArrayList<String> tableList = dbObj.getTableList();
		for (String t : tableList) {
			System.out.println(t);
		}
	}
	
	/*

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
	public void testSetIterableTable() {
		fail("Not yet implemented");
	}
	*/

}
