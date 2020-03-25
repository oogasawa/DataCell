package net.ogalab.datacell.util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;

import org.apache.commons.configuration.ConfigurationException;

public class DataChecker {
	
	protected String dbName = null;
	protected String dataSet = null;

	protected Pattern pInterruptive = Pattern.compile("(\\s+(\\S.+\\S))|((\\S.+\\S)\\s+)|(\\s+(\\S.+\\S)\\s+)");
	
	/*
	public static void main(String[] args) {
		DataChecker obj = new DataChecker(new MySQLFactory());
		obj.setDbName("rgm3");
		obj.setDataSet("package");

		try {
			obj.checkData();
		} catch (ConfigurationException e) {
			RuntimeExceptionUtil.invoke(e, "Runtime error in DataChecker.main() .");
		}
	}
	*/
	
	
	
	public void checkData(DCContainerFactory fobj, String dbName, String dataSet) throws ConfigurationException {
		DCContainer  dbObj = fobj.getInstance(dbName);
		setDbName(dbName);
		setDataSet(dataSet);
		
		ArrayList<String> predicateList = dbObj.getPredicateList(dataSet);
		for (String pred : predicateList) {
			System.out.println("# Checking " + dbName + ", "+ dataSet + ", " + pred + " ...");
			detectInterruptiveSpaces(dbObj, pred);
		}
		dbObj.close();
	}
	
	public void detectInterruptiveSpaces(DCContainer dbObj, String pred) {
		Matcher m = null;
		for (DataCell row : dbObj.setIterableTable(dataSet, pred)) {
			if (row.getID() == null) {
				System.out.println("Null ID:\t" + dataSet + ", " + pred + "\t" + row.getID() + "\t" + row.getValue());
			}
			else {
				m = pInterruptive.matcher(row.getID());
				if (m.matches()) {
					System.out.println("Interruptive spaces in IDs:\t" + dataSet + ", " + pred + "\t" + "\"" + row.getID() + "\"");
				}				
			}
			if (row.getValue() == null) {
				System.out.println("Null Value:\t" + dataSet + ", " + pred + "\t" + row.getID() + "\t" + row.getValue());
			}
			else {
				m = pInterruptive.matcher(row.getValue());
				if (m.matches()) {
					System.out.println("Interruptive spaces in Values:\t" + dataSet + ", " + pred + "\t" + "\"" + row.getValue() + "\"");
				}				
			}
		}
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDataSet() {
		return dataSet;
	}

	public void setDataSet(String dataSet) {
		this.dataSet = dataSet;
	}

}
