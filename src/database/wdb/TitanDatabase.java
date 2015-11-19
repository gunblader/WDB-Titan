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
    private String databasePath;
    private TitanGraph graph;
    
    public TitanDatabase(String databasePath) throws Exception
    {
        this.databasePath = databasePath;   
    } 

    public void openSecDb(IndexDef index) throws Exception
    {
        //open indexed graph
    }

    public void openDb() throws Exception
    {
        this.graph = TitanFactory.build().
                set("storage.backend", "berkeleyje").
                set("storage.directory", databasePath).
                open();
    } 
    public void closeDb() throws Exception
    {
        graph.close();
    }

    public DatabaseAdapter newTransaction() throws Exception
    {
        TitanTransaction tx = graph.newTransaction();
        return new TitanDatabaseAdapter(this, tx);
    }
}
