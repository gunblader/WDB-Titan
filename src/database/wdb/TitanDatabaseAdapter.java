package wdb;

import com.thinkaurelius.titan.core.TitanTransaction;
import wdb.metadata.ClassDef;
import wdb.metadata.IndexDef;
import wdb.metadata.WDBObject;

import java.util.ArrayList;

public class TitanDatabaseAdapter implements DatabaseAdapter {


    public TitanDatabaseAdapter(TitanDatabase database, TitanTransaction transaction) {

    }

    @Override
    public void commit() throws Exception {

    }

    @Override
    public void abort() throws Exception {

    }

    @Override
    public void putClass(ClassDef classDef) throws Exception {

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
