/*
 * Created on Feb 2, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package wdb;

import wdb.metadata.*;

import org.apache.commons.configuration.*;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.frames.FrameInitializer;
import com.tinkerpop.frames.FramedGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.modules.gremlingroovy.GremlinGroovyModule;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;
import com.tinkerpop.frames.modules.typedgraph.TypedGraphModuleBuilder;

import com.sleepycat.bind.*;
import com.sleepycat.je.*;

import java.io.*;
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
	
	
///////////////////////****************************************************************************************////////////////
public Configuration getTitanConf() {
Configuration conf = new BaseConfiguration();
conf.setProperty("storage.backend","cassandrathrift");
conf.setProperty("storage.hostname","127.0.0.1");

conf.setProperty("storage.index.search.backend", "elasticsearch");
conf.setProperty("storage.index.search.hostname", "127.0.0.1");
conf.setProperty("storage.index.search.client-only", "true");        

return conf;
}

public TitanGraph getTitanGraph() {
	//Open the database. Create it if it does not already exist.
TitanGraph g = TitanFactory.open(getTitanConf());

return g;
}


/*
* 
*/
//public FramedGraph<TitanGraph> getFramedTitanGraph() {
//TitanGraph g = getTitanGraph();
//
//FramedGraphFactory factory = new FramedGraphFactory(new GremlinGroovyModule(), new JavaHandlerModule());
//FramedGraph<TitanGraph> framedGraph = factory.create(g);        
//
//return framedGraph;
//}

///////////////////////****************************************************************************************////////////////

	
//	public SleepyCatDataBase(String fileName) throws Exception
//	{
//		this.fileName = fileName;
//		this.envConfig = new EnvironmentConfig();
//		this.envConfig.setTransactional(true);
//		this.envConfig.setAllowCreate(true);
//		this.env = new Environment(new File(this.fileName), this.envConfig);
//		this.classKeyPrefix = "class";
//		this.objectKeyPrefix = "object";
//		this.secDbs = new Hashtable<String, SecondaryDatabase>();
//	}
	
//	public void openDb(String dbName) throws Exception
//	{
//		//Open the database. Create it if it does not already exist.
//		this.dbName = dbName;
//		this.dbConfig = new DatabaseConfig();
//		this.dbConfig.setTransactional(true);
//		this.dbConfig.setAllowCreate(true);
//		this.dbConfig.setSortedDuplicates(false);
//		this.objectDb = this.env.openDatabase(null, dbName+"_objects", this.dbConfig);
//		this.classDb = this.env.openDatabase(null, dbName+"_classes", this.dbConfig);
//		
//		this.classCatalogDbConfig = new DatabaseConfig();
//		this.classCatalogDbConfig.setAllowCreate(true);
//		Database classCatalogDb = this.env.openDatabase(null, "class_catalog", this.classCatalogDbConfig);
//		this.classCatalog = new StoredClassCatalog(classCatalogDb);
//		
//		Cursor cursor = this.classDb.openCursor(null, null);
//		
//		EntryBinding keyBinding = new SerialBinding(this.classCatalog, String.class);
//		EntryBinding dataBinding = new SerialBinding(this.classCatalog, ClassDef.class);
//		
//		DatabaseEntry theKey = new DatabaseEntry();
//		keyBinding.objectToEntry(this.classKeyPrefix, theKey);
//		
//	    DatabaseEntry theData = new DatabaseEntry();
//	    
//	    OperationStatus status = cursor.getSearchKeyRange(theKey, theData, LockMode.DEFAULT);
//	    if(status == OperationStatus.SUCCESS && cursor.count() > 0)
//	    {
//	    	do
//	    	{
//	    		ClassDef classDef = (ClassDef)dataBinding.entryToObject(theData);
//	    		IndexDef[] indexes = classDef.getIndexes();
//	    		for(int i = 0; i < indexes.length; i++)
//	    		{
//	    			this.openSecDb(indexes[i]);
//	    		}
//	    		status = cursor.getNext(theKey, theData, LockMode.DEFAULT);
//	    	}
//	    	while(status == OperationStatus.SUCCESS);
//	    }
//	    
//	    cursor.close();
//	}
	
//	public void openSecDb(IndexDef index) throws Exception
//	{
//		this.secDbConfig = new SecondaryConfig();
//		this.secDbConfig.setTransactional(true);
//		this.secDbConfig.setAllowCreate(true);
//		this.secDbConfig.setAllowPopulate(true);
//		this.secDbConfig.setSortedDuplicates(!index.unique);
//		//this.secDbConfig.setDuplicateComparator(String.class);
//		this.secDbConfig.setKeyCreator(new SleepyCatKeyCreater(index, this.objectDb, this.classCatalog));
//		
//		SecondaryDatabase secDb = this.env.openSecondaryDatabase(null, index.name, this.objectDb, this.secDbConfig);
//		this.secDbs.put(index.name, secDb);
//		//return secDb;
//	}
	
//	public DatabaseAdapter newTransaction() throws Exception
//	{
//		Transaction txn = env.beginTransaction(null, null);
//		return new SleepyCatDataAdapter(this, txn);
//	}
	
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
