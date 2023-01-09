package net.ogalab.datacell.bdb;

import static org.junit.Assert.*;

import java.util.ArrayList;

import net.ogalab.datacell.container.DCContainer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BDBTest0 {
	
	String      dbName = "berkeleydbtest.db";
	DCContainer dbObj  = null;
	
	@Before
	public void setUp() throws Exception {
		BDBFactory facObj = new BDBFactory();
		dbObj  = facObj.getInstance(dbName);
		
		dbObj.deleteAllDCTables();
	}

	@After
	public void tearDown() throws Exception {
		dbObj.close();
	}

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
	

}
