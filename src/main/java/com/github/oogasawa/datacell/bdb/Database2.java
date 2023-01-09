package com.github.oogasawa.datacell.bdb;

import java.util.ArrayList;
import java.util.List;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseStats;
import com.sleepycat.je.DiskOrderedCursor;
import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.je.JoinConfig;
import com.sleepycat.je.JoinCursor;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.VerifyConfig;

import com.github.oogasawa.datacell.bdb.Environment2;


public class Database2 {

	Environment2 env = null;
	Database database = null;
	ArrayList<Cursor2> openedCursor = new ArrayList<Cursor2>();
	
	Database2(Database database, Environment2 env) {
		this.database = database;
		this.env = env;
	}

	public void close() {
		for (Cursor2 cursor: openedCursor) {
			try {
				cursor.close();
			} catch(DatabaseException e) {
				//do nothing
			}
		}
		database.close();
	}
	
	public int compareDuplicates(DatabaseEntry entry1, DatabaseEntry entry2) {
		return(database.compareDuplicates(entry1, entry2));
	}
	
	public int compareKeys(DatabaseEntry entry1, DatabaseEntry entry2) {
		return(database.compareKeys(entry1, entry2));
	}
	
	public long count() {
		return(database.count());
	}
	
	public OperationStatus delete(Transaction txn, DatabaseEntry key) {
		return(database.delete(txn, key));
	}
	
	public OperationStatus get(Transaction txn, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(database.get(txn, key, data, lockMode));
	}
	
	public DatabaseConfig getConfig() {
		return(database.getConfig());
	}
	
	public String getDatabaseName() {
		return(database.getDatabaseName());
	}
	
	public Environment2 getEnvironment() {
		return this.env;
	}
	
	public OperationStatus getSearchBoth(Transaction txn, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(database.getSearchBoth(txn, key, data, lockMode));
	}
	
	public List<SecondaryDatabase> getSecondaryDatabases() {
		return(database.getSecondaryDatabases());
	}
	
	public DatabaseStats getStats(StatsConfig config) {
		return(database.getStats(config));
	}
	
	public JoinCursor join(Cursor2[] cursors2, JoinConfig config) {
		Cursor cursors[] = new Cursor[cursors2.length];
		int i = 0;
		for (Cursor2 cursor: cursors2) {
			cursors[i] = cursor.getCursor();
			++i;
		}
		return(database.join(cursors, config));
	}
	
	public DiskOrderedCursor openCursor(DiskOrderedCursorConfig cursorConfig) {
		return(database.openCursor(cursorConfig));
	}
	
	public Cursor2 openCursor(Transaction txn, CursorConfig cursorConfig) {
		Cursor cursor = database.openCursor(txn, cursorConfig);
		Cursor2 cursor2 = new Cursor2(cursor, this);
		openedCursor.add(cursor2);
		return cursor2;
	}

	public Sequence openSequence(Transaction txn, DatabaseEntry key, SequenceConfig config) {
		return(database.openSequence(txn, key, config));
	}

	public void preload(long maxBytes) {
		database.preload(maxBytes);
	}
	
	public void preload(long maxBytes, long MaxMillisecs) {
		database.preload(maxBytes, MaxMillisecs);
	}
	
	public PreloadStats preload(PreloadConfig config) {
		return(database.preload(config));
	}
	
	public OperationStatus put(Transaction txn, DatabaseEntry key, DatabaseEntry data) {
		return(database.put(txn, key, data));
	}
	
	public OperationStatus putNoDupData(Transaction txn, DatabaseEntry key, DatabaseEntry data) {
		return(database.putNoDupData(txn, key, data));
	}
	
	public OperationStatus putNoOvewrite(Transaction txn, DatabaseEntry key, DatabaseEntry data) {
		return(database.putNoOverwrite(txn, key, data));
	}
	
	public void removeSequence(Transaction txn, DatabaseEntry key) {
		database.removeSequence(txn, key);
	}
	
	public void sync() {
		database.sync();
	}
	
	public DatabaseStats verify(VerifyConfig config) {
		return(database.verify(config));
	}
	
	public Database getDatabase() {
		return this.database;
	}
}
