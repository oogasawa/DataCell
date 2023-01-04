package com.github.oogasawa.datacell.mem;

import com.github.oogasawa.datacell.container.AbstractDCContainerFactory;
import com.github.oogasawa.datacell.container.DCContainer;

import org.apache.commons.configuration2.ex.ConfigurationException;



public class MemDBFactory extends AbstractDCContainerFactory {
	
	String dbName = null;

	@Override
	public DCContainer getInstance(String dbName) throws ConfigurationException {
		DCContainer dbObj = new MemDB(dbName);
		return dbObj;
	}

	public void createDB(String dbName) {
		this.dbName = dbName;
	}


	public void deleteDB(String dbName) {
		this.dbName = null;
	}


	public boolean hasDB(String dbName) {
		
		// MemDB can have at most one database in one DBContainerFactory object.
		
		if (this.dbName == null)
			return false;
		else if (this.dbName.equals(dbName))
			return true;
		else
			return false;
	}

}
