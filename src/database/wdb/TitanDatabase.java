/*
 * Created on Feb 2, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package wdb;

import wdb.metadata.ClassDef;
import wdb.metadata.IndexDef;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.Cursor;

import java.io.File;
import java.util.*;

public class TitanDatabase implements DatabaseTool {
	
	private String directory;
	
	public TitanDatabase(String directory) throws Exception
	{
		this.directory = directory;
		
	}
	
	public void openDb(String database) throws Exception
	{
//		String db = "berkeleyje";
		//Open the database. Create it if it does not already exist.
		TitanGraph g = TitanFactory.build().	
				set("storage.backend", database).
				set("storage.directory", directory).
				open();
		return g;
	}
	
	public void openSecDb(IndexDef index) throws Exception
	{
		
	}
	
	public DatabaseAdapter newTransaction() throws Exception
	{
//		Transaction txn = env.beginTransaction(null, null);
//		return new SleepyCatDataAdapter(this, txn);
		
		TitanTransaction tx = graph.newTransaction();
		return new TitanDatabaseAdapter(this, tx);
	}
	
	
	
	public void closeDb() throws Exception
	{
		graph.close();
	}
	
}
