package wdb;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import org.apache.tinkerpop.gremlin.structure.T;
import wdb.metadata.*;
import com.tinkerpop.blueprints.Vertex;

import java.util.ArrayList;
import java.util.Arrays;

public class TitanDatabaseAdapter implements DatabaseAdapter {

    private TitanTransaction tx;
    private TitanDatabase db;

    public TitanDatabaseAdapter(TitanDatabase database, TitanTransaction transaction) {
        this.db = database;
        this.tx = transaction;
    }

    @Override
    public void commit() throws Exception {
        this.tx.commit();
    }

    @Override
    public void abort() throws Exception {
        this.tx.rollback();
    }

    @Override
    public void putClass(ClassDef classDef) throws Exception {
        //Vertex x = tx.addVertex(T.label, "classDef", "name", classDef.name, "comment", classDef.comment, "attributes", classDef.attributes,
         //       "instances", classDef.instances, "indexes", classDef.indexes);

        if(classDef instanceof SubclassDef) {
            // include superclasses
            TitanVertex cd = tx.addVertex(T.label, "ClassDef", "name", classDef.name, "comment", classDef.comment);
        }
        else {
            for(Integer instances: classDef.instances)
                    TitanVertex cd = tx.addVertex(T.label, "ClassDef", "name", classDef.name, "comment", classDef.comment);
        }

    }



    }

    @Override
    public ClassDef getClass(String className) throws Exception {
        /*
        TitanGraphIndex index = mgmt.getGraphIndex(className);
        String name = index.name();
        String comment = index.getParametersFor("comment")[0];
        return new ClassDef(name, comment){
            this.name = name;
            this.comment = comment;
            this.attributes = new ArrayList<Attribute>(Arrays.asList(index.getParametersFor("attributes")));
            this.instances = new ArrayList<Integer>(Arrays.asList(index.getParametersFor("instances")));
            this.indexes = new ArrayList<IndexDef>(Arrays.asList(index.getParametersFor("indexes")));
        };
        */
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
