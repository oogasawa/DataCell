package com.github.oogasawa.datacell.bdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;

import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import org.apache.commons.configuration2.ex.ConfigurationException;

//import com.sleepycat.je.Environment;
//import com.sleepycat.je.Database;
//import com.sleepycat.je.Cursor;

import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.AbstractDCContainer;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;
import com.github.oogasawa.datacell.container.NameConverter;

import com.github.oogasawa.datacell.bdb.Environment2;
import com.github.oogasawa.datacell.bdb.Database2;
import com.github.oogasawa.datacell.bdb.Cursor2;


public class BDB extends AbstractDCContainer {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell.bdb");
	
	String dbName = null;
	
	Environment2 environment = null;
	
	// 現在openされているテーブルを管理 (テーブル名 => テーブルオブジェクト)
	// 書き込みのたびにテーブル(BDB用語ではデータベース)のopen, closeを行うことを避ける。
	TreeMap<String, Database2>  openedTables    = new TreeMap<String, Database2>();
	

	Iter    iter       = null;

	
	// simple test.
	public static void main(String[] args) {
		DCContainerFactory facObj = new BDBFactory();
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance("bdb_main_test");
			
			dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix");
			dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9091", "scientific name", "Coturnix coturnix japonica");
			dbObj.putRowIfKeyValuePairIsAbsent("ncbi_taxonomy", "9774", "scientific name", "Sirenia");
			
			for (DataCell cell : dbObj.setIterableTable("ncbi_taxonomy", "scientific name")) {
				System.out.println(cell.asJSON4());
			}
		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "main", e);

		} finally {
			if (dbObj != null)
				dbObj.close();
		}
	}
	
	
	
	
	
	public BDB() {
		super();
		nameConverter = new NameConverter();
		nameConverter.setDatabase(this);
	}


	public void appendRow(String tableName, String key, String value) {
		Database2 table  = getTableObj(tableName);
		Cursor2    cursor = null; 

		try {
			cursor = table.openCursor(null, null);

			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(key,   theKey);
			StringBinding.stringToEntry(value, theValue);

			cursor.put(theKey, theValue); 
		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "appendRow", e);
			logger.severe("An error occurred appending a row : " + key + ", " + value);
		}
		finally {
			try {
				cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "appendRow", e);
				logger.severe("An error occurred appending a row : " + key + ", " + value);
			}
		}
	}
	
	/** Returns a table object of given name.
	 * 
	 * If the object was found in the openedTables container, this method returned it.
	 * Otherwise, new table object is created.
	 * When a table with a given name does not exists, null is returned.
	 * 
	 * @param tableName
	 * @return a table object. (in the terminology of BDB, it is an object of the Database class.)
	 */
	private Database2 getTableObj(String tableName) {
		
		Database2 table = null;
		if (openedTables.containsKey(tableName)) {
			table = openedTables.get(tableName);
		}
		else {
			table = openTable(tableName);
		}
		return table;
	}
	
	private Database2 openTable(String tableName) {
		Database2          table   = null;
		
	    DatabaseConfig  dbConfig  = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    dbConfig.setSortedDuplicates(true);
	    
	    // Make it deferred write
	    //dbConfig.setDeferredWrite(true);
	    try {
			table = environment.openDatabase(null, tableName, dbConfig);
			openedTables.put(tableName, table);
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "openTable", e);
			logger.severe("An error occurred opening a table : " + dbName + ", " + tableName);
		}
	    
	    return table;
		
	}

	@Override
	public void closeIteratorIfExists() {
		if (iter != null)
			iter.dispose();
	}
	

	public void close() {
		try {
			if (iter != null)
				iter.dispose();
			if (environment != null) {
				closeTables(); 
				environment.close();
			}
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "close", e);
			logger.severe("An error occurred closing a database : " + dbName);
		} catch (Exception e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "close", e);
			logger.severe("An error occurred closing a database : " + dbName);
		}
	}

	private void closeTables() {

		Set<String> ts = openedTables.keySet();
		if (iter != null)
			iter.dispose();

		try {
			for (String t : ts) {
				logger.info(t);
				// A table is a database in the BerkeleyDB.
				// So, "Database" class object written here is a "table" in the terminology of DataCell containers.
				Database2 d = openedTables.get(t);
				if (d != null) {
					// here!!
					// The database handle should not be closed while any other handle that refers to it is not yet closed; 
					// for example, database handles should not be closed while cursor handles into the database remain open, 
					// or transactions that include operations on the database have not yet been committed or aborted.
					// Specifically, this includes Cursor and Transaction handles. 
					// http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/Database.html#close%28%29
					d.close();	
				}
			}
			
			ts.clear();
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "closeTables", e);
			logger.severe("An error occurred closing tables : " + dbName);
		} catch (Exception e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "closeTables", e);
			logger.severe("An error occurred closing tables : " + dbName);
		}
	}

	
	@Override
	public void createTable(String tableName) {
		Database2          table   = null;
		
	    DatabaseConfig  dbConfig  = new DatabaseConfig();
	    dbConfig.setAllowCreate(true);
	    dbConfig.setSortedDuplicates(true);
	    
	    // Make it deferred write
	    //dbConfig.setDeferredWrite(true);
	    try {
			table = environment.openDatabase(null, tableName, dbConfig);
			openedTables.put(tableName, table);
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "createTable", e);
			logger.severe("An error occurred creating a table : " + dbName + ", " + tableName);
		}
	}

	
	public void deleteID(String tableName, String id) {
		Database2 table  = getTableObj(tableName);
		Cursor2    cursor = null;
		
		try {
			cursor = table.openCursor(null, null);

			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(id,   theKey);
			

			// position the cursor.
			@SuppressWarnings("unused")
			OperationStatus retVal = cursor.getSearchKey(theKey, theValue, LockMode.DEFAULT);
			int hitNum = cursor.count();
			for (int i=0; i<hitNum; i++) {
				cursor.delete();
				cursor.getNextDup(theKey, theValue, LockMode.DEFAULT);
			}

		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "deleteID", e);
			logger.severe("An error occurred deleting a row : " + id );
		}
		finally {
			try {
				if (cursor != null)
					cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "deleteID", e);
				logger.severe("An error occurred deleting a row : " + id );
			}
		}

	}


	public void deleteRow(String tableName, String key, String value) {
		Database2 table  = getTableObj(tableName);
		Cursor2    cursor = null;
		
		try {
			cursor = table.openCursor(null, null);

			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(key,   theKey);
			StringBinding.stringToEntry(value, theValue);

			// position the cursor.
			@SuppressWarnings("unused")
			OperationStatus retVal = cursor.getSearchKey(theKey, theValue, LockMode.DEFAULT);
			int hitNum = cursor.count();
			for (int i=0; i<hitNum; i++) {
				cursor.delete();
				cursor.getNextDup(theKey, theValue, LockMode.DEFAULT);
			}

		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "deleteRow", e);
			logger.severe("An error occurred deleting a row : " + key );
		}
		finally {
			try {
				if (cursor != null)
					cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "deleteRow", e);
				logger.severe("An error occurred deleting a row : " + key );
			}
		}
	}

	@Override
	public void deleteTable(String tableName) {
		try {
			if (hasTableInAllTables(tableName)) {
				//getTableObj(tableName).close();
				environment.removeDatabase(null, tableName);
			}
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "deleteTable", e);
			logger.severe("An error occurred deleting a table : " + tableName );
		}

	}


	public ArrayList<String> getListOfAllTables() {
		ArrayList<String> result = new ArrayList<String>();
		try {
			List<?> list = environment.getDatabaseNames();
			for (Object item : list) {
				result.add(item.toString());
			}
			
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "getListOfAllTables", e);
			logger.severe("An error occurred getting table list.");
		}
		
		return result;

	}


	public ArrayList<String> getValueList(String tableName, String key) {

		ArrayList<String> result = new ArrayList<String>();

		Database2 table  = getTableObj(tableName);
		Cursor2    cursor = null;

		try {
			cursor = table.openCursor(null, null);

			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(key,   theKey);

			// position the cursor.
			OperationStatus retVal = cursor.getSearchKey(theKey, theValue, LockMode.DEFAULT);
			if (retVal == OperationStatus.SUCCESS) {
				int hitNum = cursor.count();
				for (int i=0; i<hitNum; i++) {
					cursor.getCurrent(theKey, theValue, LockMode.DEFAULT);
					result.add(StringBinding.entryToString(theValue));
					cursor.getNextDup(theKey, theValue, LockMode.DEFAULT);
				}
			}
		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "getValueList", e);
			logger.severe("An error occurred getting value list : " + key );
		}
		finally {
			try {
				if (cursor != null)
					cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "getValueList", e);
				logger.severe("An error occurred getting value list : " + key );
			}
		}

		return result;

	}


	public boolean hasID(String tableName, String id) {
		boolean result = false;
		
		Database2 table  = getTableObj(tableName);
		Cursor2    cursor = null;
		
		try {
			cursor = table.openCursor(null, null);
			
			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(id,   theKey);
			
			// position the cursor.
			OperationStatus retVal = cursor.getSearchKey(theKey, theValue, LockMode.DEFAULT);
			if (retVal == OperationStatus.SUCCESS)
				result = true;
		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "hasID", e);
			logger.severe("An error occurred searching for a key : " + id );
		}
		finally {
			try {
				if (cursor != null)
					cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "hasID", e);
				logger.severe("An error occurred searching for a key : " + id );
			}
		}

		return result;
	}

	
	public boolean hasRow(String tableName, String id, String value) {
		boolean result = false;
		
		Database2 table  = getTableObj(tableName);
		Cursor2   cursor = null;
		
		try {
			cursor = table.openCursor(null, null);
			
			DatabaseEntry theKey   = new DatabaseEntry();
			DatabaseEntry theValue = new DatabaseEntry();
			StringBinding.stringToEntry(id,   theKey);
			StringBinding.stringToEntry(value, theValue);
			
			// position the cursor.
			OperationStatus retVal = cursor.getSearchBoth(theKey, theValue, LockMode.DEFAULT);
			if (retVal == OperationStatus.SUCCESS)
				result = true;
		}
		catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "hasRow", e);
			logger.severe("An error occurred searching for a key,value pair : " + id + ", " + value );
		}
		finally {
			try {
				if (cursor != null)
					cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "hasRow", e);
				logger.severe("An error occurred searching for a key,value pair : " + id + ", " + value );
			}
		}

		return result;
	}


	
	public Iterator<DataCell> iterator() {
		if (iter != null)
			iter.dispose();
		
		NameConverter nc = this.getNameConverter();
		ArrayList<String> n = nc.parseTableName(iterableTable);
		return new Iter(iterableTable, 
				nc.getOriginalName(n.get(0)),
				nc.getOriginalName(n.get(1)));
	}
	
	private class Iter implements Iterator<DataCell> {
		
		Database2 table = null;
		Cursor2    cursor = null;
		
		String dataSet = null;
		String predicate = null;
		
		int state = 0;
		
		
		@SuppressWarnings("unused")
		public Database2 getTable() {
			return table;
		}
		
		@SuppressWarnings("unused")
		public void setCursor(Cursor2  c) {
			cursor = c;
		}
		
		@SuppressWarnings("unused")
		public Cursor2  getCursor() {
			return cursor;
		}
		
		public Iter(String tableName, String ds, String pred) {
			dataSet   = ds;
			predicate = pred;
			
			table  = getTableObj(tableName);
			try {
				cursor = table.openCursor(null, null);
			}
			catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "Iter private class constructor", e);
				logger.severe("An error occurred instantiating iterator.");
			}	
		}


		public boolean hasNext() {
			boolean result = false;

			try {
				DatabaseEntry theKey = new DatabaseEntry();
				DatabaseEntry theData = new DatabaseEntry();

				OperationStatus retVal = null;
				if (state == 0) {
					retVal = cursor.getFirst(theKey, theData, LockMode.DEFAULT);
					if (retVal == OperationStatus.SUCCESS) {
						result = true;
					}
				}
				else {
					retVal = cursor.getNext(theKey, theData, LockMode.DEFAULT);
					if (retVal == OperationStatus.SUCCESS) {
						result = true;
						cursor.getPrev(theKey, theData, LockMode.DEFAULT);
					}
				}

			}
			catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "Iter::hasNext", e);
				logger.severe("Unexpected error : Iter::hasNext().");
			}

			return result;
		}


		public DataCell next() {

			DataCell row = new DataCell();

			try {
				DatabaseEntry theKey   = new DatabaseEntry();
				DatabaseEntry theValue = new DatabaseEntry();

				OperationStatus retVal = null;
				if (state == 0) {
					retVal = cursor.getFirst(theKey	, theValue, LockMode.DEFAULT);
					if (retVal == OperationStatus.SUCCESS) {
						row.setDataSet(dataSet);
						row.setID(StringBinding.entryToString(theKey));
						row.setPredicate(predicate);
						row.setValue(StringBinding.entryToString(theValue));
						state = 1;
					}
					else {
						row = null;
					}

				}
				else {
					retVal = cursor.getNext(theKey, theValue, LockMode.DEFAULT);
					if (retVal == OperationStatus.SUCCESS) {
						row.setDataSet(dataSet);
						row.setID(StringBinding.entryToString(theKey));
						row.setPredicate(predicate);
						row.setValue(StringBinding.entryToString(theValue));
					}
					else {
						row = null;
					}
				}
			}
			catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "Iter::next", e);
				logger.severe("Unexpected error : Iter::next().");
			}

			return row;
		}


		public void remove() {
			try {
				cursor.delete();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "Iter::remove", e);
				logger.severe("Unexpected error : Iter::remove().");
			}
		}
		
		
		public void dispose() {
			try {
				cursor.close();
			} catch (DatabaseException e) {
				logger.throwing("com.github.oogasawa.datacell.bdb.BDB", "Iter::dispose", e);
				logger.severe("Unexpected error : Iter::dispose().");
			}
		}
		
		
		@Override
		protected void finalize() {
			try {
				super.finalize();
				if (cursor != null)
					cursor.close();
			}
			catch (Throwable e) {
				//e.printStackTrace();
			}
			finally {
				dispose();
			}
		}
		
		
	}

	public Environment2 getEnvironment() {
		return environment;
	}

	public void setEnvironment(Environment2 environment) {
		this.environment = environment;
	}





	public ArrayList<String> getIDList() {
		// TODO Auto-generated method stub
		return null;
	}




}
