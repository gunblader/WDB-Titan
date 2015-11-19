
package wdb;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;


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

        final PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).make();
        TitanManagement.IndexBuilder nameIndexBuilder = mgmt.buildIndex("name", Vertex.class).addKey(name);
        if (uniqueNameCompositeIndex)
            nameIndexBuilder.unique();

        final PropertyKey time = mgmt.makePropertyKey("comment").dataType(String.class).make();
        final PropertyKey time = mgmt.makePropertyKey("attributes").dataType(ArrayList<Attribute>.class).make();
        final PropertyKey time = mgmt.makePropertyKey("instances").dataType(ArrayList<Integer>.class).make();
        final PropertyKey time = mgmt.makePropertyKey("indexes").dataType(ArrayList<IndexDef>.class).make();
        if (null != mixedIndexName)
            mgmt.buildIndex("vertices", Vertex.class).addKey("comment").addKey("attributes").addKey("instances")
                    .addKey("indexes").buildMixedIndex(mixedIndexName);

        mgmt.makeVertexLabel("classDef").make();

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
