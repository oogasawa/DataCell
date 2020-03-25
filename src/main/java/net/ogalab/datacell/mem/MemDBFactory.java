package net.ogalab.datacell.mem;

import net.ogalab.datacell.container.AbstractDCContainerFactory;
import net.ogalab.datacell.container.DCContainer;

import org.apache.commons.configuration.ConfigurationException;

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