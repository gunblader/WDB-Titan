/*
 * Created on Feb 2, 2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package wdb;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import wdb.metadata.ClassDef;
import wdb.metadata.IndexDef;

public class TitanDatabase implements DatabaseTool {
	
	private String directory;
	private TitanGraph graph;
	
	public TitanDatabase(String directory) throws Exception
	{
		this.directory = directory;
		
	}
	
	public void openDb(String database) throws Exception
	{
//		String db = "berkeleyje";
		//Open the database. Create it if it does not already exist.
		this.graph = TitanFactory.build().
				set("storage.backend", database).
				set("storage.directory", directory).
				open();
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
