package wdb;

import com.thinkaurelius.titan.core.TitanTransaction;
import wdb.metadata.ClassDef;
import wdb.metadata.IndexDef;
import wdb.metadata.WDBObject;

import java.util.ArrayList;

public class TitanDatabaseAdapter implements DatabaseAdapter {

	private TitanTransaction tx;
	private TitanDatabase db;

    public TitanDatabaseAdapter(TitanDatabase database, TitanTransaction transaction) {
    	this db = database;
    	this tx = transaction;
    }

    @Override
    public void commit() throws Exception {

    }

    @Override
    public void abort() throws Exception {

    }

    @Override
    public void putClass(ClassDef classDef) throws Exception {
        
    	Vertex x = tx.addVertex(T.label, "classDef", "name", classDef.name, "comment", classDef.comment, "attributes", classDef.attributes,
    			 "instances", classDef.instances, "indexes", classDef.indexes);
    	
		this.scdb.getClassDb().put(this.txn, theKey, theData);
    }

    @Override
    public ClassDef getClass(String className) throws Exception {
        return null;
    }

    @Override
    public void putObject(WDBObject object) throws Exception {

    }

    @Override
    public ArrayList<WDBObject> getObjects(IndexDef index, String key) throws Exception {
        return null;
    }

    @Override
    public WDBObject getObject(String className, Integer Uid) throws Exception {
        return null;
    }
}
