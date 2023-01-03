package com.github.oogasawa.datacell.util;

import java.util.ArrayList;
import java.util.logging.Logger;

import com.github.oogasawa.datacell.container.DCContainer;
import com.github.oogasawa.datacell.container.DCContainerFactory;

import org.apache.commons.configuration.ConfigurationException;

public class DataRemover {

	private static final Logger logger = Logger.getLogger("com.github.oogasawa.datacell");
	
	// protected Parameter param = new Parameter();
	protected DCContainerFactory facObj = null;

	// HashSet<String> blackList = new HashSet<String>();

	public DataRemover(DCContainerFactory facObj) {
		this.facObj = facObj;
	}

	public void pass() {
	}

	public void cui(String[] args) {
		// String[] args2 = setParameters(args, "DataRemover");

		try {
			if (args.length == 1) // e.g. DataRemover RGM3
				// printDataSets(args2); // remove database ?? No. not implementd.
				pass();
			else if (args.length == 2) // e.g. DataRemover RGM3 "R CC package"
				removeDataSet(args);
			else if (args.length >= 3) { // e.g. DataRemover RGM3 "R CC package" "DESCRIPTON file path"
				removeTable(args);
			}

		} catch (Exception e) {
			logger.throwing("com.github.oogasawa.datacell.util.DataRemover", "cui", e);
		}
	}

	/*
	 * public void test(String[] args) { String format = "tdf2"; DataCell cell = new
	 * DataCell(); cell.setFormat(format); int[] range = getRange(); printRows(new
	 * String[] {"example_db", "example", "value"}, cell, range); }
	 */

	/*
	 * private String[] setParameters(String[] args, String progName) {
	 * 
	 * param.setDefault("Format", "TDF2"); param.setDefault("Range", "0,10"); //
	 * range =(from,to) or range="all"
	 * 
	 * String[] otherArgs = null; // Set parameters on the object.
	 * param.setDefault("ProgramName", progName); try { otherArgs =
	 * param.parseCommandLine(args, progName); } catch (Exception e) {
	 * e.printStackTrace(); } param.printValidParameters();
	 * 
	 * return otherArgs; }
	 */

	public void removeDataSet(String[] args) {
		String dbName = args[0];
		String dataSet = args[1];

		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);

			ArrayList<String> predList = dbObj.getPredicateList(dataSet);
			for (String pred : predList) {
				if (pred == null)
					continue;
				logger.info("removeDataSet: removing\t" + dataSet + "\t" + pred);
				dbObj.deleteTable(dataSet, pred);
			}

		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.datacell.util.DataRemover", "removeDataSet", e);
			logger.severe("Can not open the database " + dbName);
		} finally {
			if (dbObj != null)
				dbObj.close();
		}

	}

	public void removeTable(String[] args) {
		String dbName = args[0];
		String dataSet = args[1];
		String predicate = args[2];

		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);

			logger.info("removeDataSet: removing\t" + dataSet + "\t" + predicate);
			dbObj.deleteTable(dataSet, predicate);

		} catch (ConfigurationException e) {
			logger.throwing("com.github.oogasawa.datacell.util.DataRemover", "removeTable", e);
			logger.severe("Can not open the database " + dbName);
		} finally {
			if (dbObj != null)
				dbObj.close();
		}

	}

}
