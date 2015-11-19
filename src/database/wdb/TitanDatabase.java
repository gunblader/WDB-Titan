
package wdb;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Vertex;

import org.apache.tinkerpop.gremlin.structure.T;
import wdb.metadata.Attribute;
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
        createSchema();
    }

    private void createSchema() throws Exception
    {
        TitanManagement mgmt = this.graph.openManagement();

        //TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        //if (uniqueNameCompositeIndex)
        //    nameIndexBuilder.unique();

        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("comment").dataType(String.class).make();
        mgmt.makePropertyKey("required").dataType(Boolean.class).make();
        mgmt.makePropertyKey("unique").dataType(Boolean.class).make();
        mgmt.makePropertyKey("type").dataType(String.class).make();
        mgmt.makePropertyKey("size").dataType(Integer.class).make();
        mgmt.makePropertyKey("baseClassName").dataType(String.class).make();
        mgmt.makePropertyKey("classDefName").dataType(String.class).make();
        mgmt.makePropertyKey("inverseEVA").dataType(String.class).make();
        mgmt.makePropertyKey("cardinality").dataType(Integer.class).make();
        mgmt.makePropertyKey("distinct").dataType(Boolean.class).make();
        mgmt.makePropertyKey("max").dataType(Integer.class).make();
        mgmt.makePropertyKey("uid").dataType(Integer.class).make();
        mgmt.makePropertyKey("superclasses").dataType(String.class).cardinality(Cardinality.LIST).make();

        mgmt.makeEdgeLabel("attributeOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("instanceOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("indexOf").multiplicity(Multiplicity.MULTI).make();

        mgmt.makeEdgeLabel("parents").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("children").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("evaObjects").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("dvaObjects").multiplicity(Multiplicity.MULTI).make();

        mgmt.makeVertexLabel("ClassDef").make();
        //p String name
        //p String comment
        //e ArrayList<Attribute> attributes
        //e ArrayList<Integer> instances
        //e ArrayList<IndexDef> indexes

        mgmt.makeVertexLabel("WDBObject").make();
	    //p String classDefName
	    //p Integer Uid
	    //e Hashtable<String, Integer> parents
	    //e Hashtable<String, Integer> children
	    //e Hashtable<String, Object> evaObjects
	    //e Hashtable<String, Object> dvaValues

        mgmt.makeVertexLabel("IndexDef").make();
        //p String name
        //p String comment
        //p String className
        //p Boolean unique

        mgmt.makeVertexLabel("Attribute").make();
        //p String name
        //p String comment
        //p Boolean required

        mgmt.makeVertexLabel("DVA").make();
        //p String type
        //p Integer size
        // Object initialValue  // string | integer | boolean ??

        mgmt.makeVertexLabel("EVA").make();
        //p String baseClassName
        //p String inverseEVA
        //p Integer cardinality
        //p Boolean distinct
        //p Integer max


        mgmt.commit();

        mgmt = graph.openManagement();
        System.out.println(mgmt.containsVertexLabel("classDef"));

        //if (null != mixedIndexName)
         //   mgmt.buildIndex("vertices", Vertex.class).addKey("comment").addKey("attributes").addKey("instances")
          //          .addKey("indexes").buildMixedIndex(mixedIndexName);

        mgmt.commit();
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
