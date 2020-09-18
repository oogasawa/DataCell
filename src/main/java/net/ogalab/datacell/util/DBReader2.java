package net.ogalab.datacell.util;

import java.util.ArrayList;

import org.apache.commons.configuration.ConfigurationException;

import net.ogalab.datacell.DataCell;
import net.ogalab.datacell.container.DCContainer;
import net.ogalab.datacell.container.DCContainerFactory;


public class DBReader2 {
	
	DCContainerFactory facObj = null;
	
	String dbName  = null;
	String dataSet = null;
	String predicate = null;
	
	public DBReader2(DCContainerFactory facObj) {
		this.facObj = facObj;
	}
	
	/** Data setをテキスト形式で出力する。
	 * <h3>SYNOPSIS</h3>
	 * <pre>{@code 
	 * DBReader2 dbName data_set 
	 * }</pre>
	 * 
	 * 出力の書式をコントロールするような設定ファイルを指定できた方がよい。
	 * <ul>
	 * <li>表示するpredicateのリストを指定できる方がよい。
	 * <li>表示する際にpredicateの名前を置き換えられる方がよい。
	 * </ul>
	 * 
	 * @param args String型の配列<br>
	 * 要素1: データベース名<br>
	 * 要素2: データセット名<br>
	 * 要素3: predicate名
	 */
	
	public void cui(String[] args) {
		dbName    = args[0];
		dataSet   = args[1];
		predicate = args[2];
		
		printText(dbName,  dataSet, predicate);
	}
	

	
	public void printText(String dbName, String dataSet, String pred) {
		DCContainer dbObj = null;
		try {
			dbObj = facObj.getInstance(dbName);
			
			int count = 0;
			
			for (DataCell cell : dbObj.setIterableTable(dataSet, pred)) {
				String id    = cell.getID();
				//String value = cell.getValue();
				
				System.out.println("#--- " + ++count);
				
				System.out.println("@ data set: " + dataSet);
				System.out.println("@ ID: " + id);
				
				ArrayList<String> predList = dbObj.getPredicateList(dataSet);
				for (String p : predList) {
					ArrayList<String> valueList = dbObj.getValueList(dataSet, id, p);
					for (String v : valueList) {
						if (isMultiLine(v)) {
							System.out.println("@ " + p);
							System.out.println("@#begin");
							System.out.println(v);
							System.out.println("@#end");
						}
						else {
							System.out.println("@@ " + p + ": " + v);							
						}
					}

				}
				
			}
			
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (dbObj != null)
				dbObj.close();
		}
	}

	public boolean isMultiLine(String str) {
		return str.contains("\n");
	}
	
}
