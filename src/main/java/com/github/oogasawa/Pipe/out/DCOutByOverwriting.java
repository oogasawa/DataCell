package com.github.oogasawa.Pipe.out;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.commons.configuration.ConfigurationException;

import com.github.oogasawa.Pipe.Out;
import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;
import com.github.oogasawa.utility.types.string.StringUtil;


public class DCOutByOverwriting implements Out {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.Pipe");
	
	private DCContainer dbObj = null; 
	private String ds = null;
	private String pred = null;
	
	public DCOutByOverwriting(DCContainerFactory facObj, String dbName, String ds, String pred) throws IOException {
		try {
			dbObj = facObj.getInstance(dbName);
		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.Pipe.out.DCOutByOverwriting", "constructor", e);
			logger.warning("Runtime exception in a DCOutByOverwriting constructor. ");
		}
		this.ds   = ds;
		this.pred = pred;
	}
	

	
	public void putLine(String line) {
		ArrayList<String> col = StringUtil.splitByTab(line);
		dbObj.putRowWithReplacingValues(ds, col.get(0), pred, col.get(1));
	}

	public void end() {
		if (dbObj != null)
			dbObj.close();
	}

	public Object get() {
		return null;
	}
	

}
