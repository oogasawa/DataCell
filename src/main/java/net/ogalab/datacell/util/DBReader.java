package net.ogalab.datacell.util;

import java.util.ArrayList;
import java.util.HashSet;

import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;
import net.ogalab.util.cli.Parameter;
import net.ogalab.util.exception.RuntimeExceptionUtil;
import net.ogalab.util.fundamental.StringUtil;
import net.ogalab.util.fundamental.Type;

import org.apache.commons.configuration.ConfigurationException;


/**
 * 
 * <h3>Example of main() methods</h3>
 * <pre>{@code
 *     public static void main(String[] args) {
 *   	
 *   	DBReader obj = new DBReader();
 *   	obj.cui(args);
 *   	// obj.test(args);
 *  }
 *  }</pre>
 * 
 * @author oogasawa
 *
 */
public class DBReader {
	
	protected Parameter       param  = new Parameter();
	protected DCContainerFactory facObj = null;
	
	HashSet<String> blackList = new HashSet<String>();
	
	
    public DBReader(DCContainerFactory facObj) {
    	this.facObj = facObj;
    }
    
    
    public void cui(String[] args) {
		String[] args2 = setParameters(args, "DBReader");
		
//		try {
			if (args2.length == 1)
				printDataSets(args2);
			else if (args2.length == 2)
				printPredicates(args2);
			else if (args2.length == 3) {
				String format = param.getString("Format").toLowerCase();
				if (param.getString("Range").equals("all")) {
					printRows(args2, format);
				}
				else {
					int[] range = getRange();
					printRows(args2, format, range);
				}
			}
			else if (args2.length >= 4) {
				String format = param.getString("Format").toLowerCase();
				DataCell cell = new DataCell();
				cell.setFormat(format);
				printValues(args2);
			}

//		}
//		catch (Exception e) {
//			RuntimeExceptionUtil.invoke(e);
//		}    	
    }
    
    
    public void test(String[] args) {
    	String format = "tsv2";
		int[] range = getRange();
		printRows(new String[] {"example_db", "example", "value"}, format, range);
    }
    
    
	public DBReader() {
		blackList.add("INTERNAL_NAME");
		blackList.add("ORIGINAL_NAME");
		blackList.add("INTERNAL_NAME_PREFIX");
	}
    
    
	private String[] setParameters(String[] args, String progName) {
		
		param.setDefault("Format",    "JSON4");
		param.setDefault("Range",     "0,10"); // range =(from,to) or range="all"

		String[] otherArgs = null;
	    // Set parameters on the object.
		param.setDefault("ProgramName", progName);
		try {
		otherArgs = param.parseCommandLine(args, progName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		param.printValidParameters();

		return otherArgs;
	}
	
	
	public void printDataSets(String[] args) {
		String dbName = args[0];
		
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);
			
			int counter = 0;
			
			ArrayList<String> dsList = dbObj.getDataSetList();
			for (String ds : dsList) {
				if (ds == null)
					continue;
				if (blackList.contains(ds))
					continue;
				ArrayList<String> predList = dbObj.getPredicateList(ds);
				counter++;
				System.out.println(counter + "\t" + ds + "\t" + predList.size());
			}
			
		} catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
		}
		finally {
			dbObj.close();
		}

	}
	
	
	public void printPredicates(String[] args) {
		String dbName = args[0];
		String dataSet = args[1];
		
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);
			
			int counter = 0;
			
			ArrayList<String> predList = dbObj.getPredicateList(dataSet);
			for (String pred : predList) {
				if (!blackList.contains(dataSet) && !blackList.contains(pred)) {
					int numOfRows = getNumOfRows(dbObj, dataSet, pred);
					System.out.println(++counter + "\t" + dataSet + "\t" + pred + "\t" + numOfRows);
				}
			}
			
		} catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
		}
		finally {
			dbObj.close();
		}

	}


	private int getNumOfRows(DCContainer dbObj, String dataSet, String pred) {
		int num = 0;
		for (@SuppressWarnings("unused") 
			DataCell row : dbObj.setIterableTable(dataSet, pred))
			num++;
		return num;
	}
	
	
	public int[] getRange() {
		int[] result = new int[2];
		ArrayList<String> range = StringUtil.splitByComma(param.getString("Range"));
		result[0] = Type.to_int(range.get(0).trim());
		result[1] = Type.to_int(range.get(1).trim());
		return result;
	}

	
	public void printRows(String[] args, String format) {
		String dbName = args[0];
		String dataSet = args[1];
		String pred    = args[2];
		
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);

			int counter = 0;
			for (DataCell cell : dbObj.setIterableTable(dataSet, pred)) {
				
				if (format.equalsIgnoreCase("JSON4") || format.equalsIgnoreCase("JSON2")) {
					System.out.println("#--- : " + counter);
				}
				System.out.println(cell);
				counter++;
			}
		}
		catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
		}
		finally {
			dbObj.close();
		}

	}

	
	public void printRows(String[] args, String format, int[] range) {
		String dbName = args[0];
		String dataSet = args[1];
		String pred    = args[2];
		int    start   = range[0];
		int    end     = range[1];
		
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);

			int counter = 0;
			for (DataCell cell : dbObj.setIterableTable(dataSet, pred)) {

				if (counter > end) {
					break;
				}
				if (counter >= start) {

					if (format.equalsIgnoreCase("JSON4") || format.equalsIgnoreCase("JSON2")) 
						System.out.println("#--- : " + counter);
					cell.setFormat(format);
					System.out.println(cell);
				}
				
				counter++;
			}
		}
		catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
		}
		finally {
			if (dbObj != null)
				dbObj.close();
		}

	}

	public void printValues(String[] args) {
		String dbName = args[0];
		String dataSet = args[1];
		String pred    = args[2];
		String key     = args[3];
		
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);

			ArrayList<String> valueList = dbObj.getValueList(dataSet, key, pred);
			for (String val : valueList) {
				int counter = 0;				
				
				System.out.println("#--- : " + counter);
				System.out.println(val);
				
				counter++;
			}
		}
		catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "ERROR: Can not open the database " + dbName);
		}
		finally {
			dbObj.close();
		}

	}
	
}
