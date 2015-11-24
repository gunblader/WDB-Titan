
package wdb;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import wdb.metadata.IndexDef;


public class TitanDatabase implements DatabaseTool {
    private String databasePath;
    public TitanGraph graph;


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

        /*
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        TitanGraphIndex namei = mgmt.buildIndex("name", Vertex.class).addKey(name).unique().buildCompositeIndex();
        mgmt.setConsistency(namei, ConsistencyModifier.LOCK);
        */

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

        mgmt.makePropertyKey("id").dataType(String.class).make();
        mgmt.makeEdgeLabel("superclassOf").multiplicity(Multiplicity.MULTI).make();

        mgmt.makeEdgeLabel("instanceOf").multiplicity(Multiplicity.MULTI).make();

        mgmt.makeEdgeLabel("attributeOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("indexOf").multiplicity(Multiplicity.MULTI).make();

        mgmt.makeEdgeLabel("parent").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("child").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("evaOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("dvaOf").multiplicity(Multiplicity.MULTI).make();

        mgmt.commit();
    }
    public void closeDb() throws Exception
    {
        graph.close();
    }

    public DatabaseAdapter newTransaction() throws Exception
    {
        return new TitanDatabaseAdapter(graph.newTransaction());
    }
}
