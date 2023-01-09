package com.github.oogasawa.datacell.bdb;

import java.io.File;
import java.io.PrintStream;
import java.util.List;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.PreloadStats;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionStats;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VersionMismatchException;

public class Environment2 {

	Environment env = null;

	public Environment2(File envHome, EnvironmentConfig configuration) throws EnvironmentNotFoundException,
			EnvironmentLockedException, VersionMismatchException, DatabaseException, IllegalArgumentException {

		env = new Environment(envHome, configuration);
	}

	public Transaction beginTransaction(Transaction parent, TransactionConfig txnConfig) {
		return (env.beginTransaction(parent, txnConfig));
	}

	public void checkpoint(CheckpointConfig ckptConfig) {
		env.checkpoint(ckptConfig);
	}

	public int cleanLog() {
		return (env.cleanLog());
	}

	public void close() {
		env.close();
	}

	public void compress() {
		env.compress();
	}

	public void evictMemory() {
		env.evictMemory();
	}

	public void flushLog(boolean fsync) {
		env.flushLog(fsync);
	}

	public EnvironmentConfig getConfig() {
		return (env.getConfig());
	}

	public List<String> getDatabaseNames() {
		return (env.getDatabaseNames());
	}

	public File getHome() {
		return (env.getHome());
	}

	public LockStats getLockStats(StatsConfig config) {
		return (env.getLockStats(config));
	}

	public EnvironmentMutableConfig getMutableConfig() {
		return (env.getMutableConfig());
	}

	public EnvironmentStats getStats(StatsConfig config) {
		return (env.getStats(config));
	}

	public Transaction getThreadTransaction() {
		return (env.getThreadTransaction());
	}

	public TransactionStats getTransactionStats(StatsConfig config) {
		return (env.getTransactionStats(config));
	}

	public boolean isValid() {
		return (env.isValid());
	}

	public Database2 openDatabase(Transaction txn, String databaseName, DatabaseConfig dbConfig) {
		Database database = env.openDatabase(txn, databaseName, dbConfig);
		Database2 database2 = new Database2(database, this);
		return database2;
	}

	public SecondaryDatabase openSecondaryDatabase(Transaction txn, String databaseName, Database2 primaryDatabase,
			SecondaryConfig dbConfig) {
		Database database = primaryDatabase.getDatabase();
		return (env.openSecondaryDatabase(txn, databaseName, database, dbConfig));
	}

	public PreloadStats preload(Database[] databases, PreloadConfig config) {
		return (env.preload(databases, config));
	}

	public void printStartupInfo(PrintStream out) {
		env.printStartupInfo(out);
	}

	public void removeDatabase(Transaction txn, String databaseName) {
		env.removeDatabase(txn, databaseName);
	}

	public void renameDatabase(Transaction txn, String databaseName, String newName) {
		env.renameDatabase(txn, databaseName, newName);
	}

	public void setMutableConfig(EnvironmentMutableConfig mutableConfig) {
		env.setMutableConfig(mutableConfig);
	}

	public void setThreadTransaction(Transaction txn) {
		env.setThreadTransaction(txn);
	}

	public void sync() {
		env.sync();
	}

	public void truncateDatabase(Transaction txn, String databaseName, boolean returnCount) {
		env.truncateDatabase(txn, databaseName, returnCount);
	}

	public boolean verify(VerifyConfig config, PrintStream out) {
		return (env.verify(config, out));
	}
}
