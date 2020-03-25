package net.ogalab.datacell.container;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractDCContainerFactory implements DCContainerFactory {

	Logger logger = LoggerFactory.getLogger(AbstractDCContainerFactory.class);
	
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
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (dbObj != null)
				dbObj.close();
		}
	}
	
}
