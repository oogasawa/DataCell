package com.github.oogasawa.datacell.bdb;

import java.io.File;
import java.io.FileFilter;
import java.util.logging.Logger;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.sleepycat.je.DatabaseException;
//import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import com.github.oogasawa.datacell.container.AbstractDCContainerFactory;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.utility.files.FileUtil;
import com.github.oogasawa.datacell.bdb.Environment2;

public class BDBFactory extends AbstractDCContainerFactory {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell.bdb");
	
	@Override
	public DCContainer getInstance(String dbName) throws ConfigurationException {
		BDB dbObj = new BDB();
		
	    // Open the environment. Create it if it does not already exist.
	    EnvironmentConfig envConfig = new EnvironmentConfig();
	    envConfig.setAllowCreate(true);
	    if (!FileUtil.exists(dbName))
	    	FileUtil.mkdirs(dbName);
	    try {
			dbObj.setEnvironment(new Environment2(new File(dbName), envConfig));
			
		} catch (DatabaseException e) {
			logger.throwing("com.github.oogasawa.datacell.bdb.BDBFactory", "getInstance", e);
			logger.severe("An error occurred opening a database : " + dbName);
		}
		
		return dbObj;
	}
	
	
	public void createDB(String dbName) {
		// By default, if the database specified in the URL does not yet exist, 
		// a new (empty) database is created automatically.
		// The user that created the database automatically becomes the administrator of this database.

		// Therefore ...,  nothing to do.
	}


	public void deleteDB(String dbName) {
		// To delete database, delete the database file by using an OS command.
		File dir = new File(dbName);
		File[] cont = dir.listFiles();
		for (File f : cont)
			f.delete();
		dir.delete();
	}


	public boolean hasDB(String dbName) {
		File dir = new File(dbName);
		if (dir.exists() && dir.isDirectory())
			return true;
		else
			return false;
	}

/*
	public void deleteDB(String dbName) {
		// To delete database, delete the database file by using an OS command.
		File dir = new File(".");
		FileFilter fileFilter = new WildcardFileFilter(dbName + "*.db");
		File[] files = dir.listFiles(fileFilter);
		for (File f : files) {
		   f.delete();
		}
	}


	public boolean hasDB(String dbName) {
		File dir = new File(".");
		FileFilter fileFilter = new WildcardFileFilter(dbName + "*.db");
		File[] files = dir.listFiles(fileFilter);
		if (files == null || files.length == 0)
			return false;
		else
			return true;
	}
*/	

}
