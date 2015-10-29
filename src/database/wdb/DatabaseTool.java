package wdb;

import wdb.metadata.IndexDef;

public interface DatabaseTool {

	String dbName = null;
	String fileName = null;
	// used in SleepyCatDataBase -> this
	// used in WDB.java
	void openSecDb(IndexDef index) throws Exception;

	// used in WDB.java
	DatabaseAdapter newTransaction() throws Exception;
	void closeDb() throws Exception;
	void openDb(String dbName) throws Exception;
}
