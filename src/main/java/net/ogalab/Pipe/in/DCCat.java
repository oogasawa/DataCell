package net.ogalab.Pipe.in;

import java.util.Iterator;

import net.ogalab.Pipe.In;
import net.ogalab.Pipe.Pipe;
import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.util.exception.RuntimeExceptionUtil;

import org.apache.commons.configuration.ConfigurationException;


public class DCCat implements In {
	//private BufferedReader reader;
	private DCContainer   dbObj = null;
	public Iterator<DataCell> iter = null;
	
	public DCCat(String dbName, String dataset, String predicate, DCContainerFactory facObj)  {
		try {
			dbObj = facObj.getInstance(dbName);
			dbObj.setIterableTable(dataset, predicate);
		} catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "Runtime error in DCCat constructor. ");
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
			RuntimeExceptionUtil.invoke(e, "Runtime error in CDBCat.getLine() ");
		}
		
		return result;
	}
	
	
	public void close() {
		if (dbObj != null) {
			dbObj.close();
		}
	}


}
