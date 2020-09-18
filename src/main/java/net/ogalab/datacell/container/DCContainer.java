package net.ogalab.datacell.container;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import net.ogalab.datacell.DataCell;


public interface DCContainer extends Iterable<DataCell> {
	
	/*
	 * Create and Update 
	 */
	/** Store a row (a data cell) to a database 
	 * without checking the existence of the database, table, and key duplication.
	 * 
	 * @param tableName テーブル名
	 * @param id id列の値
	 * @param value value列の値
	 */
	public void appendRow(String tableName, String id, String value);	

	public void appendRow(String dataset, String id, String pred, String value);
	
	public void appendRow(DataCell cell);
	
	public boolean getAutoCommit();
	
	public void setAutoCommit(boolean ac);
	
	public void commit();
	
	public void close();
	
	
	public void createTable(String tableName);
	
	public void createTable(String ds, String pred);
	
	public void createTable(DataCell cell);
	
	public void createTableIfAbsent(String tableName);
	
	public void createTableIfAbsent(String ds, String pred);
	
	public void createTableIfAbsent(DataCell cell);


	
	/*
	 * Delete
	 */
	
	/** Delete all rows of a given key.
	 * 
	 * @param dataset データセット名
	 * @param id id列の値
	 * @param pred predicate名
	 */
	public void deleteID(String dataset, String id, String pred);
	
	/** Delete all rows of a given key specified by a DataCell object.
	 * 
	 * This method is equivalent with the following code:
	 * <pre>{@code
	 *deleteKey(cell.getDataSet(), cell.getID(), cell.getPredicate());
	 *}</pre>
	 * 
	 * @param cell 消去する行を示すDataCell object
	 */
	public void deleteID(DataCell cell);
	
	public void deleteID(String tableName, String id);

	public void deleteRow(String tableName, String id, String value);
	
	public void deleteRow(String dataset, String id, String pred, String value);
	
	public void deleteRow(DataCell cell);
	
	public void deleteTable(String tableName);
	

	public void deleteTable(String ds, String pred);

	/** Delete a table specified by a given data cell object.
	 * 
	 * This method is equivalent with the following code:
	 * <pre>{@code
	 *deleteTable(cell.getDataSet(), cell.getPredicate());
	 *}</pre>
	 * 
	 * @param cell 消去するテーブルを示すDataCell object
	 */
	public void deleteTable(DataCell cell);
	
	public void deleteTableIfExists(String tableName);
	
	public void deleteTableIfExists(String ds, String pred);

	/** Delete a table specified by a given data cell object.
	 * 
	 * This method is equivalent with the following code:
	 * <pre>{@code
	 *deleteTableIfExists(cell.getDataSet(), cell.getPredicate());
	 *}</pre>
	 * 
	 * @param cell 消去する行を示すDataCell object
	 */
	public void deleteTableIfExists(DataCell cell);
	
	public void deleteAllDCTables();
	

	/*
	public void deleteDataset(String dataset);
	
	public void deleteKeyFromAllPredicates(String dataset, String key);
	
	public void deletePredicate(String dataset, String predicate);
	
	public void deletePredicate(String dataset, String key, String predicate);
	*/
	
	
	/*
	 * Read and search
	 */
	
	/** Returns a list of all the DataCell objects stored in the database.
	 * 
	 * @param ds   A data set.
	 * @param pred A predicate.
	 * @return A list of data cells.
	 */
	public ArrayList<DataCell> getDataCellList(String ds, String pred);
	
	/** Returns an iterator object, 
	 * which can iterate through all the data cells with a given data set and a predicate.
	 * 
	 * @param ds   A data set.
	 * @param pred A predicate
	 * @return     An iterator of data cells
	 */
	public Iterator<DataCell> getDataCellIterator(String ds, String pred);
	
	public ArrayList<DataCell> getDataCellList(String ds, String id, String pred);
	
	//public ArrayList<ArrayList<String>> getRowList(String dataset, String pred); // rowのリストの取得
	
    //public Iterator<ArrayList<String>> getRowListAsIterator(String dataset, String pred);
    
	/** Returns a list of internal names of data cell related tables.
	 * 
	 * This method does not returns the names of internally used tables, that is:
	 * <ul>
	 * <li>ORIGINAL_NAME__INTERNAL_NAME
	 * <li>INTERNAL_NAME__ORIGINAL_NAME
	 * <li>INTERNAL_NAME_PREFIX__MAX_COUNT
	 * </ul>
	 * 
	 * This method also does not returns the tables
	 * which is not managed by the data cell container, 
	 * if the database contains those irrelevant tables.
	 * 
	 * @return internal names of ordinary tables.
	 */
    public ArrayList<String> getTableList();
    
    /** Returns a list of all tables in a database.
     * 
     * Internally used tables, and the tables irrelevant to the data cell containers
     * may be contained in the result of this method.
     * 
     * @return データベース中の全テーブルのリスト
     */
    public ArrayList<String> getListOfAllTables();
    
    public ArrayList<ArrayList<String>> getDsPredPairs();
 
    
    /** Returns original names of all data sets in the database.
     * 
     * @return list of original names of data sets.
     */
    public ArrayList<String> getDataSetList();
    
    
    
    /** Returns original names of predicates of a given dataset.
     * 
     * @param ds original name of dataset.
     * @return list of original names of predicates.
     */
    public ArrayList<String> getPredicateList(String ds);
    
    
    public ArrayList<String> getValueList(String ds, String id, String pred);

    public ArrayList<String> getValueList(String tableName, String id);

    
    public String getValue(String ds, String id, String pred);
    
    public String getValue(String tableName, String id);
    
    public DataCell getDataCell(String ds, String id, String pred);
    

    public boolean hasID(String ds, String id, String pred);
	
	public boolean hasID(String tableName, String id);
	
	public boolean hasID(DataCell cell);
	
	public boolean hasRow(String dataset, String id, String pred, String value);
	
	public boolean hasRow(String tableName, String id, String value);
	
	public boolean hasRow(DataCell cell);
	
	public boolean hasTableInAllTables(String tableName);
	
	public boolean hasTable(String ds, String pred);
	
	public boolean hasTable(DataCell cell);
	
	public Iterator<DataCell> iterator();
	
	/** DB中にデータの重複があるかどうかチェックせずにデータを保存する。テーブルが存在していなければ作成する.
	 * 
	 * @param tableName テーブル名
	 * @param id id列の値
	 * @param value value列の値
	 */
	public void putRow(String tableName, String id, String value);
	
	public void putRow(String tableName, String id, String value, boolean trim);
	
	
	/** DB中にデータの重複があるかどうかチェックせずにデータを保存する。テーブルが存在していなければ作成する.
	 * 
	 * @param ds データセット名
	 * @param id id列の値
	 * @param pred predicate名
	 * @param value value列の値
	 */
	public void putRow(String ds, String id, String pred, String value);
	
	public void putRow(DataCell cell);
	
	public void putRowIfKeyValuePairIsAbsent(String tableName, String key, String value);
	
	public void putRowIfKeyValuePairIsAbsent(String ds, String key, String pred, String value);
	
	public void putRowIfKeyValuePairIsAbsent(DataCell cell);
	
	public void putRowIfKeyIsAbsent(String tableName, String key, String value); // keyが一致したら何もしない
	
	public void putRowIfKeyIsAbsent(String ds, String key, String pred, String value); // keyが一致したら何もしない
	
	public void putRowIfKeyIsAbsent(DataCell cell);
	
	public void putRowByOverwriting(String tableName, String id, String value); // keyが一致したらvalueを上書き

	public void putRowByOverwriting(String ds, String id, String pred, String value); // keyが一致したらvalueを上書き
	
	public void putRowByOverwriting(DataCell cell);
	
	public void putRow(String ds, String id, String pred, String value, boolean trim);
	
	public void putRow(DataCell cell, boolean trim);
	
	public void putRowIfKeyValuePairIsAbsent(String tableName, String id, String value, boolean trim);
	
	public void putRowIfKeyValuePairIsAbsent(String ds, String id, String pred, String value, boolean trim);
	
	public void putRowIfKeyValuePairIsAbsent(DataCell cell, boolean trim);
	
	public void putRowIfKeyIsAbsent(String tableName, String id, String value, boolean trim); // keyが一致したら何もしない
	
	public void putRowIfKeyIsAbsent(String ds, String id, String pred, String value, boolean trim); // keyが一致したら何もしない
	
	public void putRowIfKeyIsAbsent(DataCell cell, boolean trim);
	
	public void putRowByOverwriting(String tableName, String id, String value, boolean trim); // keyが一致したらvalueを上書き

	public void putRowByOverwriting(String ds, String id, String pred, String value, boolean trim); // keyが一致したらvalueを上書き

	public void putRowByOverwriting(DataCell cell, boolean trim);
		
	//public String getDataset(String tableName);
	
	//public String getPredicate(String tableName);
	
	//public String getTableName(String dataset, String predicate);
	
	//public String getReversedTableName(String tableName);
	
	//public void printTable(String tableName);
	
	//public void saveTable(String tableName, String filename);
	
	
	/** DB中の全データセットのoriginal nameを返す 
	 * 
	 * @return 全データセットのoriginal nameのリスト
	 */
	public ArrayList<String> getDatasets();
	
	public ArrayList<String> getPredicates(String ds);
	
	
	/*
	 * 
	 */
	public DCContainer setIterableTable(String ds, String pred);
	
	public DCContainer setIterableTable(String tableName);
	
	//public Iterator<ArrayList<String>> getNewIterator();
	
	public String getInternalName(String originalName);
	
	public String getOriginalName(String internalName);
	
	public int getPrefixMaxCount(String prefix);
	
	public NameConverter getNameConverter();
	
	public boolean isValidTableName(String tableName);
	
	
	//----------

	public Set<String> getIDSet(String ds, String pred);
	
	public Set<String> getIDSet(String ds, String[] pred);
	
	public Set<String> getIDSet(String ds, ArrayList<String> pred);
	

	public ArrayList<String> getIDList(String ds, String pred);
	
	public ArrayList<String> getIDList(String ds, String[] pred);
	
	public ArrayList<String> getIDList(String ds, ArrayList<String> pred);
	

	
	// ----------
	public String getDCText(String ds, ArrayList<String> predList);
	
	public String getDCText(String ds, String[] predList);
	
	public String getDCText(String ds, String pred);
	
	
	
	public String getDCText(String ds, String id, ArrayList<String> predList);
	
	public String getDCText(String ds, String id, String[] predList);
	
	public String getDCText(String ds, String id, String pred);

	
	public String getDCText(String ds, String[] id, ArrayList<String> predList);
	
	public String getDCText(String ds, String[] id, String[] predList);
	
	public String getDCText(String ds, String[] id, String pred);

	
	public String getDCText(String ds, ArrayList<String> id, ArrayList<String> predList);
	
	public String getDCText(String ds, ArrayList<String> id, String[] predList);
	
	public String getDCText(String ds, ArrayList<String> id, String pred);
	
	public void readDCText(String dctext);
	
	public void readDCText(ArrayList<String> lines);
	
	public void readDCText(String dctext, String defaultDs);
	
	public void readDCText(ArrayList<String> lines, String defaultDs);
	
}
