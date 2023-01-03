package com.github.oogasawa.datacell.container;

import org.apache.commons.configuration.ConfigurationException;

public interface DCContainerFactory {

	public DCContainer getInstance(String dbName) throws ConfigurationException;
	
	public String getAuthPath();

	public void setAuthPath(String authPath) throws ConfigurationException;
	
	public void setUsername(String user);

	public void setPassword(String pass);
	
	public void createDB(String dbName);
	
	public void createDBIfAbsent(String dbName);

	public void deleteDB(String dbName);
	
	public void deleteDBIfExists(String dbName);
	
	public void deleteTable(String dbName, String ds, String pred);
	
	public void deleteTableIfExists(String dbName, String ds, String pred);
	
	public boolean hasDB(String dbName);

}
