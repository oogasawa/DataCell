package com.github.oogasawa.datacell.bdb;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class Cursor2 {
	Database2 database = null;
	Cursor cursor = null;
	
	Cursor2(Cursor cursor, Database2 database) {
		this.cursor = cursor;
		this.database = database;
	}
	
	public void close() {
		cursor.close();
	}
	
	public int count() {
		return(cursor.count());
	}
	
	public long countEstimate() {
		return(cursor.countEstimate());
	}
	
	public OperationStatus delete() {
		return(cursor.delete());
	}
	
	public Cursor dup(boolean samePosition) {
		return(cursor.dup(samePosition));
	}
	
	public CacheMode getCacheMode() {
		return(cursor.getCacheMode());
	}
	
	public CursorConfig getConfig() {
		return(cursor.getConfig());
	}
	
	public OperationStatus getCurrent(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getCurrent(key, data, lockMode));
	}
	
	public Database2 getDatabase() {
		return this.database;
	}
	
	public OperationStatus getFirst(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getFirst(key, data, lockMode));
	}
	
	public OperationStatus getLast(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getLast(key, data, lockMode));
	}

	public OperationStatus getNext(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getNext(key, data, lockMode));
	}

	public OperationStatus getNextDup(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getNextDup(key, data, lockMode));
	}

	public OperationStatus getNextNoDup(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getNextNoDup(key, data, lockMode));
	}

	public OperationStatus getPrev(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getPrev(key, data, lockMode));
	}

	public OperationStatus getPrevDup(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getPrevDup(key, data, lockMode));
	}

	public OperationStatus getPrevNoDup(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getPrevNoDup(key, data, lockMode));
	}

	public OperationStatus getSearchBoth(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getSearchBoth(key, data, lockMode));
	}

	public OperationStatus getSearchBothRange(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getSearchBothRange(key, data, lockMode));
	}

	public OperationStatus getSearchKey(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getSearchKey(key, data, lockMode));
	}

	public OperationStatus getSearchKeyRange(DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.getSearchKeyRange(key, data, lockMode));
	}

	public OperationStatus put(DatabaseEntry key, DatabaseEntry data) {
		return(cursor.put(key, data));
	}

	public OperationStatus putCurrent(DatabaseEntry data) {
		return(cursor.putCurrent(data));
	}

	public OperationStatus putNoDupData(DatabaseEntry key, DatabaseEntry data) {
		return(cursor.putNoDupData(key, data));
	}

	public OperationStatus putNoOverwrite(DatabaseEntry key, DatabaseEntry data) {
		return(cursor.putNoOverwrite(key, data));
	}

	public void setCacheMode(CacheMode cacheMode) {
		cursor.setCacheMode(cacheMode);
	}
	
	public long skipNext(long maxCount, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.skipNext(maxCount, key, data, lockMode));
	}
	
	public long skipPrev(long maxCount, DatabaseEntry key, DatabaseEntry data, LockMode lockMode) {
		return(cursor.skipPrev(maxCount, key, data, lockMode));
	}
	
	public Cursor getCursor() {
		return this.cursor;
	}
}
