package com.github.oogasawa.Pipe.in;

import java.util.Iterator;
import java.util.logging.Logger;

import com.github.oogasawa.Pipe.In;
import com.github.oogasawa.Pipe.Pipe;
import com.github.oogasawa.datacell.DataCell;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;


import org.apache.commons.configuration.ConfigurationException;


public class DCCat implements In {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.Pipe");

	//private BufferedReader reader;
	private DCContainer   dbObj = null;
	public Iterator<DataCell> iter = null;
	
	public DCCat(String dbName, String dataset, String predicate, DCContainerFactory facObj)  {
		try {
			dbObj = facObj.getInstance(dbName);
			dbObj.setIterableTable(dataset, predicate);
		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.Pipe.in.DCCat", "constructor", e);
			logger.warning("Runtime exception in a DCCat constructor. ");
		}

	}
	

	public String getLine() {
		String   result = Pipe.END;
		DataCell row    = null;
		//Iterator<ArrayList<String>> iter = dbObj.iterator();
		try {
			if (iter == null)
				iter = dbObj.iterator();
			
			if (iter.hasNext()) {
				row = iter.next();
				result = row.asTSV2();
			}
		} catch (Exception e) {
			logger.throwing("com.github.oogasawa.Pipe.in.DCCat", "constructor", e);
		}
		
		return result;
	}
	
	
	public void close() {
		if (dbObj != null) {
			dbObj.close();
		}
	}


}
