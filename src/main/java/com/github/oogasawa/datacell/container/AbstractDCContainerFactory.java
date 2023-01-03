package com.github.oogasawa.datacell.container;

import java.util.logging.Logger;

import org.apache.commons.configuration.ConfigurationException;


abstract public class AbstractDCContainerFactory implements DCContainerFactory {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");
	
	protected String authPath = null;
	
	protected String username = null;
	
	protected String password = null;
	
	public AbstractDCContainerFactory() { }
		
	abstract public DCContainer getInstance(String dbName) throws ConfigurationException;

	public void setUsername(String user) {
		username = user;
	}

	public void setPassword(String pass) {
		password = pass;
	}
	
	public String getAuthPath() {
		return authPath;
	}

	public void setAuthPath(String authPath) throws ConfigurationException {
		this.authPath = authPath;
	}


	public void createDBIfAbsent(String dbName) {
		if (!hasDB(dbName))
			createDB(dbName);
	}
	
	public void deleteDBIfExists(String dbName) {
		if (hasDB(dbName))
			deleteDB(dbName);
	}
	
	public void deleteTable(String dbName, String ds, String pred)  {
		DCContainer dbObj = null;
		try {
			dbObj = this.getInstance(dbName);
			dbObj.deleteTable(ds, pred);
		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.datacell.app.AbstractDCContainerFactory", "deleteTable", e);
		} finally {
			if (dbObj != null)
				dbObj.close();			
		}


	}
	
	public void deleteTableIfExists(String dbName, String ds, String pred)  {
		DCContainer dbObj = null;
		try {
			dbObj = this.getInstance(dbName);
			dbObj.deleteTableIfExists(ds, pred);
		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.datacell.app.AbstractDCContainerFactory", "deleteTableIfExists", e);
		} finally {
			if (dbObj != null)
				dbObj.close();
		}
	}
	
}
