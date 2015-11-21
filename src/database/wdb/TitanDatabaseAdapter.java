package wdb;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import wdb.metadata.*;

import java.util.*;

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
        System.out.println("putting class: ");
        System.out.println(classDef.name);
        System.out.println(classDef.comment);
        System.out.println(classDef.instances);
        System.out.println(classDef.attributes);
        System.out.println(classDef.indexes);

        TitanVertex classVertex = tx.addVertex("ClassDef");
        classVertex.property("name", classDef.name);
        classVertex.property("comment", classDef.comment);

        for(Integer instanceId: classDef.instances) {
            TitanVertex instanceVertex = tx.addVertex("Instance");
            instanceVertex.property("uid", instanceId);
            classVertex.addEdge("instanceOf", instanceVertex);
        }
        for(Attribute attr: classDef.attributes) {
            System.out.println("attribute: name: " + attr.name + ", comment: " + attr.comment + ", required: " + attr.required);
            TitanVertex attrVertex = tx.addVertex("Attribute");
            attrVertex.property("name", attr.name);
            attrVertex.property("comment", attr.comment);
            Boolean required = attr.required == null ? false : attr.required;
            attrVertex.property("required", required);
            classVertex.addEdge("attributeOf", attrVertex);
        }
        for(IndexDef index: classDef.indexes) {
            TitanVertex indexVertex = tx.addVertex("IndexDef");
            indexVertex.property("name", index.name);
            indexVertex.property("comment", index.comment);
            indexVertex.property("className", index.className);
            indexVertex.property("unique", index.unique);
            classVertex.addEdge("indexOf", classVertex);
        }

        if(classDef instanceof SubclassDef) {
            for(String superclassId: ((SubclassDef) classDef).superClasses) {
                TitanVertex superclassVertex = tx.addVertex("Superclass");
                superclassVertex.property("id", superclassId);
                classVertex.addEdge("superclassOf", superclassVertex);
            }
        }
    }

    @Override
    public ClassDef getClass(String className) throws Exception {
        //Iterator<TitanVertex> vertices = tx.query().labels("ClassDef").has("name", className).vertices().iterator();
        Iterator<TitanVertex> vertices = tx.query().has("name", className).vertices().iterator();

        if(!vertices.hasNext()) {
            throw new ClassNotFoundException("Class \"" + className + "\" is not defined");
        }
        TitanVertex classVertex = vertices.next();
        ClassDef classDef;
        if(classVertex.property("superclass").isPresent()) {
            SubclassDef subclassDef = new SubclassDef();
            subclassDef.superClasses = new ArrayList<>();
            Iterator<Edge> superclassIter = classVertex.edges(Direction.OUT, "superclassOf");
            while(superclassIter.hasNext()) {
                Vertex superclassVertex = superclassIter.next().inVertex();
                String id = (String) superclassVertex.property("id").value();
                subclassDef.superClasses.add(id);
            }
            classDef = subclassDef;
        }
        else {
            classDef = new ClassDef();
        }
        classDef.name = className;
        classDef.comment = (String) classVertex.property("comment").value();

        classDef.instances = new ArrayList<>();
        Iterator<Edge> instanceIter = classVertex.edges(Direction.OUT, "instanceOf");
        while(instanceIter.hasNext()) {
            Vertex instanceVertex = instanceIter.next().inVertex();
            Integer id = (Integer) instanceVertex.property("uid").value();
            classDef.instances.add(id);
        }

        classDef.attributes = new ArrayList<>();
        Iterator<Edge> attributeIter = classVertex.edges(Direction.OUT, "attributeOf");
        while(attributeIter.hasNext()) {
            Vertex attrVertex = attributeIter.next().inVertex();
            Attribute attribute = new Attribute();
            attribute.name = (String) attrVertex.property("name").value();
            attribute.comment = (String) attrVertex.property("comment").value();
            attribute.required = (Boolean) attrVertex.property("required").value();
            classDef.attributes.add(attribute);
        }

        classDef.indexes = new ArrayList<>();
        Iterator<Edge> indexIter = classVertex.edges(Direction.OUT, "indexOf");
        while(indexIter.hasNext()) {
            Vertex indexVertex = indexIter.next().inVertex();
            IndexDef indexDef = new IndexDef();
            indexDef.name = (String) indexVertex.property("name").value();
            indexDef.comment = (String) indexVertex.property("comment").value();
            indexDef.className = (String) indexVertex.property("className").value();
            indexDef.unique = (Boolean) indexVertex.property("unique").value();
            classDef.indexes.add(indexDef);
        }
        return classDef;
    }

    @Override
    public void putObject(WDBObject wdbObject) throws Exception {
        TitanVertex objectVertex = tx.addVertex("WDBObject");
        objectVertex.property("classDefName", wdbObject.getClassName());
        objectVertex.property("uid", wdbObject.getUid());

        for(Map.Entry<String, Integer> parent: wdbObject.parents.entrySet()) {
            final String parentClass = parent.getKey();
            final Integer parentObject = parent.getValue();
            TitanVertex parentVertex = tx.addVertex("Parent");
            parentVertex.property("classDefName", parentClass);
            parentVertex.property("uid", parentObject);
            objectVertex.addEdge("parent", parentVertex);
        }
        for(Map.Entry<String, Integer> child: wdbObject.children.entrySet()) {
            final String childClass = child.getKey();
            final Integer childObject = child.getValue();
            TitanVertex childVertex = tx.addVertex("Child");
            childVertex.property("classDefName", childClass); // key
            childVertex.property("uid", childObject); // value
            objectVertex.addEdge("child", childVertex);
        }

        for(Map.Entry<String, Object> evaObject: wdbObject.evaObjects.entrySet()) {
            final String id = evaObject.getKey();
            final EVA eva = (EVA) evaObject.getValue();
            TitanVertex evaVertex = tx.addVertex("EVA");
            evaVertex.property("id", id); // key
            evaVertex.property("name", eva.name);
            evaVertex.property("comment", eva.comment);
            evaVertex.property("required", eva.required);
            evaVertex.property("baseClassName", eva.baseClassName);
            evaVertex.property("inverseEVA", eva.inverseEVA);
            evaVertex.property("cardinality", eva.cardinality);
            evaVertex.property("distinct", eva.distinct);
            evaVertex.property("max", eva.max);
            objectVertex.addEdge("evaOf", evaVertex);
        }

        for(Map.Entry<String, Object> dvaValue: wdbObject.dvaValues.entrySet()) {
            final String id = dvaValue.getKey();
            final DVA dva = (DVA) dvaValue.getValue();
            TitanVertex dvaVertex = tx.addVertex("DVA");
            dvaVertex.property("id", id); // key
            dvaVertex.property("name", dva.name);
            dvaVertex.property("comment", dva.comment);
            dvaVertex.property("required", dva.required);
            dvaVertex.property("type", dva.type);
            dvaVertex.property("size", dva.size);
            if(dva.initialValue instanceof String) {
                dvaVertex.property("initValType", "String");
                dvaVertex.property("initValString", (String) dva.initialValue);
            }
            if(dva.initialValue instanceof Boolean) {
                dvaVertex.property("initValType", "Boolean");
                dvaVertex.property("initValBoolean", (Boolean) dva.initialValue);
            }
            if(dva.initialValue instanceof String) {
                dvaVertex.property("initValType", "Integer");
                dvaVertex.property("initValInteger", (Integer) dva.initialValue);
            }
            objectVertex.addEdge("dvaOf", dvaVertex);
        }
    }

    @Override
    public WDBObject getObject(String className, Integer uid) throws Exception {
        Iterator<TitanVertex> vertices = tx.query().has("label", "WDBObject")
                .has("classDefName", className)
                .has("uid", uid)
                .vertices().iterator();
        if (!vertices.hasNext()) {
            throw new ClassNotFoundException("WDBObject with className: \"" + className + "\", and uid: \"" + uid + "\", is not defined");
        }
        TitanVertex objectVertex = vertices.next();
        WDBObject wdbObject = new WDBObject();
        wdbObject.classDefName = className;
        wdbObject.Uid = uid;

        wdbObject.parents = new Hashtable<>();
        Iterator<Edge> parentsIter = objectVertex.edges(Direction.OUT, "parent");
        while (parentsIter.hasNext()) {
            Vertex parentVertex = parentsIter.next().inVertex();
            String parentClass = (String) parentVertex.property("classDefName").value();
            Integer parentObject = (Integer) parentVertex.property("uid").value();
            wdbObject.parents.put(parentClass, parentObject);
        }
        wdbObject.children = new Hashtable<>();
        Iterator<Edge> childrenIter = objectVertex.edges(Direction.OUT, "child");
        while (childrenIter.hasNext()) {
            Vertex childVertex = childrenIter.next().inVertex();
            String childClass = (String) childVertex.property("classDefName").value();
            Integer childObject = (Integer) childVertex.property("uid").value();
            wdbObject.parents.put(childClass, childObject);
        }
        wdbObject.evaObjects = new Hashtable<>();
        Iterator<Edge> evaIter = objectVertex.edges(Direction.OUT, "evaOf");
        while (evaIter.hasNext()) {
            Vertex evaVertex = evaIter.next().inVertex();
            String id = (String) evaVertex.property("id").value(); // key
            EVA eva = new EVA();
            eva.name = (String) evaVertex.property("name").value();
            eva.comment = (String) evaVertex.property("comment").value();
            eva.required = (Boolean) evaVertex.property("required").value();
            eva.baseClassName = (String) evaVertex.property("baseClassName").value();
            eva.inverseEVA = (String) evaVertex.property("inverseEVA").value();
            eva.cardinality = (Integer) evaVertex.property("cardinality").value();
            eva.distinct = (Boolean) evaVertex.property("distinct").value();
            eva.max = (Integer) evaVertex.property("max").value();
            wdbObject.evaObjects.put(id, eva);
        }

        wdbObject.dvaValues = new Hashtable<>();
        Iterator<Edge> dvaIter = objectVertex.edges(Direction.OUT, "dvaOf");
        while (dvaIter.hasNext()) {
            Vertex dvaVertex = dvaIter.next().inVertex();
            String id = (String) dvaVertex.property("id").value(); // key
            DVA dva = new DVA();
            dva.name = (String) dvaVertex.property("name").value();
            dva.comment = (String) dvaVertex.property("comment").value();
            dva.required = (Boolean) dvaVertex.property("required").value();
            dva.type = (String) dvaVertex.property("type").value();
            dva.size = (Integer) dvaVertex.property("size").value();

            String initValueType = (String) dvaVertex.property("initValueType").value();
            if ("String".equals(initValueType)) {
                dva.initialValue = dvaVertex.property("initValString").value();
            } else if ("Boolean".equals(initValueType)) {
                dva.initialValue = dvaVertex.property("initValBoolean").value();
            } else if ("Integer".equals(initValueType)) {
                dva.initialValue = dvaVertex.property("initValInteger").value();
            }
            wdbObject.evaObjects.put(id, dva);
        }
        return wdbObject;
    }

    @Override
    public ArrayList<WDBObject> getObjects(IndexDef index, String key) throws Exception {
        return null;
    }
}
