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

/**
 * @author Bo Li
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SleepyCatDataBase implements DatabaseTool {
	protected String fileName;
	protected String dbName;
	protected EnvironmentConfig envConfig;
	protected Environment env;
	protected DatabaseConfig dbConfig;
	protected Database objectDb;
	protected Database classDb;
	protected DatabaseConfig classCatalogDbConfig;
	protected StoredClassCatalog classCatalog;
	protected SecondaryConfig secDbConfig;
	protected String classKeyPrefix;
	protected String objectKeyPrefix;
	protected Hashtable<String, SecondaryDatabase> secDbs;
	
	
	public SleepyCatDataBase(String fileName) throws Exception
	{
		this.fileName = fileName;
		this.envConfig = new EnvironmentConfig();
		this.envConfig.setTransactional(true);
		this.envConfig.setAllowCreate(true);
		this.env = new Environment(new File(this.fileName), this.envConfig);
		this.classKeyPrefix = "class";
		this.objectKeyPrefix = "object";
		this.secDbs = new Hashtable<String, SecondaryDatabase>();
	}
	
	public void openDb() throws Exception
	{
		//Open the database. Create it if it does not already exist.
		TitanGraph g = TitanFactory.build().	
				set("storage.backend", "berkeleyje").
				set("storage.directory", "/tmp/graph").
				open();
		return graph;
	}
	
	public void openSecDb(IndexDef index) throws Exception
	{
		TitanGraph graph = TitanFactory.build()
				.set("storage.backend", "hbase")
				.open();
//		return graph;
	}
	
	public DatabaseAdapter newTransaction() throws Exception
	{
		Transaction txn = env.beginTransaction(null, null);
		return new SleepyCatDataAdapter(this, txn);
	}
	
	public Database getObjectDb() throws Exception
	{
		return this.objectDb;
	}
	
	public Database getClassDb() throws Exception
	{
		return this.classDb;
	}
	
	public StoredClassCatalog getClassCatalog() throws Exception
	{
		return this.classCatalog;
	}
	
	public SecondaryDatabase getSecDb(IndexDef index) throws Exception
	{
		SecondaryDatabase secDb = (SecondaryDatabase)this.secDbs.get(index.name);
		
		if(secDb == null)
		{
			throw new Exception("Index \"" + index.name + "\" is not defined");
		}
		
		return secDb;
	}
	
	public void closeDb() throws Exception
	{
		Enumeration secDbKeys = this.secDbs.keys();
		while(secDbKeys.hasMoreElements())
		{
			SecondaryDatabase secDb = (SecondaryDatabase)secDbs.get(secDbKeys.nextElement());
			secDb.close();
		}
		this.objectDb.close();
		this.classDb.close();
		this.classCatalog.close();
		this.env.close();
	}

	/**
	 * @return Returns the classKeyPrefix.
	 */
	public String getClassKeyPrefix() {
		return classKeyPrefix;
	}

	/**
	 * @return Returns the objectKeyPrefix.
	 */
	public String getObjectKeyPrefix() {
		return objectKeyPrefix;
	}
	
}
