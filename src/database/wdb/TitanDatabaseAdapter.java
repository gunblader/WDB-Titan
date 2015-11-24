package wdb;

import com.thinkaurelius.titan.core.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import wdb.metadata.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TitanDatabaseAdapter implements DatabaseAdapter {
    Logger logger = LoggerFactory.getLogger(TitanDatabaseAdapter.class);
    private TitanTransaction tx;

    public TitanDatabaseAdapter(TitanTransaction transaction) {
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
        deleteIfExists(tx.query().has("vlabel", "ClassDef").has("name", classDef.name));

        logger.info("putting class: ");
        logger.info(classDef.name);
        logger.info(classDef.comment);
        logger.info(classDef.instances.toString());
        logger.info(classDef.attributes.toString());
        logger.info(classDef.indexes.toString());

        TitanVertex classVertex = tx.addVertex();
        classVertex.property("vlabel", "ClassDef");
        classVertex.property("name", classDef.name);
        classVertex.property("comment", classDef.comment);

        for(Integer instanceId: classDef.instances) {
            TitanVertex instanceVertex = tx.addVertex();
            instanceVertex.property("uid", instanceId);
            classVertex.addEdge("instanceOf", instanceVertex);
        }

        for(Attribute attr: classDef.attributes) {
            if(attr instanceof DVA) {
                DVA dva = (DVA) attr;
                logger.info("dva: " + dva.name + " " + dva.comment + " " + dva.required + " " + dva.type + " " + dva.size);
                TitanVertex dvaVertex = tx.addVertex();
                dvaVertex.property("vlabel", "DVA");
                if(dva.name != null)
                    dvaVertex.property("name", dva.name);
                if(dva.comment != null)
                    dvaVertex.property("comment", dva.comment);
                if(dva.required != null)
                    dvaVertex.property("required", dva.required);
                if(dva.type != null)
                    dvaVertex.property("type", dva.type);
                if(dva.size != null)
                    dvaVertex.property("size", dva.size);
                if(dva.initialValue != null) {
                    if (dva.initialValue instanceof String) {
                        dvaVertex.property("initValType", "String");
                        dvaVertex.property("initValString", (String) dva.initialValue);
                    }
                    if (dva.initialValue instanceof Boolean) {
                        dvaVertex.property("initValType", "Boolean");
                        dvaVertex.property("initValBoolean", (Boolean) dva.initialValue);
                    }
                    if (dva.initialValue instanceof String) {
                        dvaVertex.property("initValType", "Integer");
                        dvaVertex.property("initValInteger", (Integer) dva.initialValue);
                    }
                }
                classVertex.addEdge("dvaOf", dvaVertex);
            }
            else { // eva
                EVA eva = (EVA) attr;
                logger.info("eva: " + eva.name + " " + eva.comment + " " + eva.required + " " + eva.baseClassName);
                TitanVertex evaVertex = tx.addVertex();
                evaVertex.property("vlabel", "EVA");
                if(eva.name != null)
                    evaVertex.property("name", eva.name);
                if(eva.comment != null)
                    evaVertex.property("comment", eva.comment);
                if(eva.required != null)
                    evaVertex.property("required", eva.required);
                if(eva.baseClassName != null)
                    evaVertex.property("baseClassName", eva.baseClassName);
                if(eva.inverseEVA != null)
                    evaVertex.property("inverseEVA", eva.inverseEVA);
                if(eva.cardinality != null)
                    evaVertex.property("cardinality", eva.cardinality);
                if(eva.distinct != null)
                    evaVertex.property("distinct", eva.distinct);
                if(eva.max != null)
                    evaVertex.property("max", eva.max);
                classVertex.addEdge("evaOf", evaVertex);
            }
        }

        for(IndexDef index: classDef.indexes) {
            TitanVertex indexVertex = tx.addVertex();
            indexVertex.property("name", index.name);
            indexVertex.property("comment", index.comment);
            indexVertex.property("className", index.className);
            indexVertex.property("unique", index.unique);
            classVertex.addEdge("indexOf", classVertex);
        }

        if(classDef instanceof SubclassDef) {
            SubclassDef subclassDef = (SubclassDef) classDef;
            logger.info("superclasses: " + subclassDef.superClasses.toString());
            for(String superclassId: subclassDef.superClasses) {
                TitanVertex superclassVertex = tx.addVertex();
                superclassVertex.property("id", superclassId);
                classVertex.addEdge("superclassOf", superclassVertex);
            }
        }
    }

    @Override
    public ClassDef getClass(String className) throws Exception {
        Iterator<TitanVertex> vertices = tx.query().has("vlabel", "ClassDef").has("name", className).vertices().iterator();

        if(!vertices.hasNext()) {
            throw new ClassNotFoundException("Class \"" + className + "\" is not defined");
        }
        TitanVertex classVertex = vertices.next();

        if(vertices.hasNext()) {
            TitanVertex nextClassVertex = vertices.next();
            logger.info("there are duplicate versions of class " + className + ": " + classVertex + " " + nextClassVertex);
        }
        ClassDef classDef;

        SubclassDef subclassDef = new SubclassDef();
        subclassDef.superClasses = new ArrayList<>();
        Iterator<Edge> superclassIter = classVertex.edges(Direction.OUT, "superclassOf");
        while(superclassIter.hasNext()) {
            Vertex superclassVertex = superclassIter.next().inVertex();
            String id = (String) superclassVertex.property("id").value();
            subclassDef.superClasses.add(id);
        }

        classDef = subclassDef.superClasses.size() > 0 ? subclassDef : new ClassDef();
        classDef.name = className;
        classDef.comment = (String) classVertex.property("comment").value();

        classDef.instances = new ArrayList<>();
        Iterator<Edge> instanceIter = classVertex.edges(Direction.OUT, "instanceOf");
        while(instanceIter.hasNext()) {
            Vertex instanceVertex = instanceIter.next().inVertex();
            Integer id = (Integer) instanceVertex.property("uid").value();
            classDef.instances.add(id);
        }
        Iterator<Edge> evaIter = classVertex.edges(Direction.OUT, "evaOf");
        while (evaIter.hasNext()) {
            Vertex evaVertex = evaIter.next().inVertex();
            EVA eva = vertexToEVA(evaVertex);
            classDef.attributes.add(eva);
        }
        Iterator<Edge> dvaIter = classVertex.edges(Direction.OUT, "dvaOf");
        while (dvaIter.hasNext()) {
            Vertex dvaVertex = dvaIter.next().inVertex();
            DVA dva = new DVA();
            dva.name = dvaVertex.property("name").isPresent() ? (String) dvaVertex.property("name").value() : null;
            dva.comment = dvaVertex.property("comment").isPresent() ? (String) dvaVertex.property("comment").value() : null;
            dva.required =  dvaVertex.property("required").isPresent() ? (Boolean) dvaVertex.property("required").value() : null;
            dva.type = dvaVertex.property("type").isPresent() ? (String) dvaVertex.property("type").value() : null;
            dva.size = dvaVertex.property("size").isPresent() ? (Integer) dvaVertex.property("size").value() : null;

            if(dvaVertex.property("initValType").isPresent()) {
                String initValType = (String) dvaVertex.property("initValType").value();
                if ("String".equals(initValType)) {
                    dva.initialValue = dvaVertex.property("initValString").value();
                } else if ("Boolean".equals(initValType)) {
                    dva.initialValue = dvaVertex.property("initValBoolean").value();
                } else if ("Integer".equals(initValType)) {
                    dva.initialValue = dvaVertex.property("initValInteger").value();
                }
            }
            classDef.attributes.add(dva);
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

        deleteIfExists(tx.query().has("vlabel", "WDBObject")
                        .has("classDefName", wdbObject.classDefName)
                        .has("uid", wdbObject.getUid()));

        logger.info("putting object: ");
        TitanVertex objectVertex = tx.addVertex();
        objectVertex.property("vlabel", "WDBObject");
        if(wdbObject.getClassName() != null)
            objectVertex.property("classDefName", wdbObject.getClassName());
        if(wdbObject.getUid() != null)
            objectVertex.property("uid", wdbObject.getUid());

        for(Map.Entry<String, Integer> parent: wdbObject.parents.entrySet()) {
            final String parentClass = parent.getKey();
            final Integer parentObject = parent.getValue();
            TitanVertex parentVertex = tx.addVertex();
            parentVertex.property("classDefName", parentClass); // key
            if(parentObject != null)
                parentVertex.property("uid", parentObject); // val
            objectVertex.addEdge("parent", parentVertex);
        }
        for(Map.Entry<String, Integer> child: wdbObject.children.entrySet()) {
            final String childClass = child.getKey();
            final Integer childObject = child.getValue();
            TitanVertex childVertex = tx.addVertex();
            childVertex.property("classDefName", childClass); // key
            if(childObject != null)
                childVertex.property("uid", childObject); // value
            objectVertex.addEdge("child", childVertex);
        }
        for(Map.Entry<String, Object> evaObject: wdbObject.evaObjects.entrySet()) {
            final String id = evaObject.getKey();
            final EVA eva = (EVA) evaObject.getValue();
            TitanVertex evaVertex = tx.addVertex();
            evaVertex.property("id", id); // key
            if(eva.name != null)
                evaVertex.property("name", eva.name);
            if(eva.comment != null)
                evaVertex.property("comment", eva.comment);
            if(eva.required != null)
                evaVertex.property("required", eva.required);
            if(eva.baseClassName != null)
                evaVertex.property("baseClassName", eva.baseClassName);
            if(eva.inverseEVA != null)
                evaVertex.property("inverseEVA", eva.inverseEVA);
            if(eva.cardinality != null)
                evaVertex.property("cardinality", eva.cardinality);
            if(eva.distinct != null)
                evaVertex.property("distinct", eva.distinct);
            if(eva.max != null)
                evaVertex.property("max", eva.max);
            objectVertex.addEdge("evaOf", evaVertex);
        }

        for(Map.Entry<String, Object> dvaValue: wdbObject.dvaValues.entrySet()) {
            final String id = dvaValue.getKey();
            final Object dvaVal = dvaValue.getValue();
            TitanVertex dvaVertex = tx.addVertex();
            dvaVertex.property("id", id); // key

            if(dvaVal != null) {
                if (dvaVal instanceof String) {
                    dvaVertex.property("initValType", "String");
                    dvaVertex.property("initValString", (String) dvaVal);
                }
                if (dvaVal instanceof Boolean) {
                    dvaVertex.property("initValType", "Boolean");
                    dvaVertex.property("initValBoolean", (Boolean) dvaVal);
                }
                if (dvaVal instanceof Integer) {
                    dvaVertex.property("initValType", "Integer");
                    dvaVertex.property("initValInteger", (Integer) dvaVal);
                }
            }
            objectVertex.addEdge("dvaOf", dvaVertex);
        }
    }

    @Override
    public WDBObject getObject(String className, Integer uid) throws Exception {
        logger.info("getting object: ");
        Iterator<TitanVertex> vertices = tx.query()
                .has("vlabel", "WDBObject")
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
            Integer parentObject = parentVertex.property("uid").isPresent() ? (Integer) parentVertex.property("uid").value() : null;
            wdbObject.parents.put(parentClass, parentObject);
        }
        wdbObject.children = new Hashtable<>();
        Iterator<Edge> childrenIter = objectVertex.edges(Direction.OUT, "child");
        while (childrenIter.hasNext()) {
            Vertex childVertex = childrenIter.next().inVertex();
            String childClass = (String) childVertex.property("classDefName").value();
            Integer childObject = childVertex.property("uid").isPresent() ? (Integer) childVertex.property("uid").value() : null;
            wdbObject.parents.put(childClass, childObject);
        }
        wdbObject.evaObjects = new Hashtable<>();
        Iterator<Edge> evaIter = objectVertex.edges(Direction.OUT, "evaOf");
        while (evaIter.hasNext()) {
            Vertex evaVertex = evaIter.next().inVertex();
            String id = (String) evaVertex.property("id").value(); // key
            wdbObject.evaObjects.put(id, vertexToEVA(evaVertex));
        }

        wdbObject.dvaValues = new Hashtable<>();
        Iterator<Edge> dvaIter = objectVertex.edges(Direction.OUT, "dvaOf");
        while (dvaIter.hasNext()) {
            Vertex dvaVertex = dvaIter.next().inVertex();
            String id = (String) dvaVertex.property("id").value(); // key
            Object dva = null;
            if(dvaVertex.property("initValType").isPresent()) {
                String initValueType = (String) dvaVertex.property("initValType").value();
                if ("String".equals(initValueType)) {
                    dva = dvaVertex.property("initValString").value();
                } else if ("Boolean".equals(initValueType)) {
                    dva = dvaVertex.property("initValBoolean").value();
                } else if ("Integer".equals(initValueType)) {
                    dva = dvaVertex.property("initValInteger").value();
                }
            }
            wdbObject.dvaValues.put(id, dva);
        }
        return wdbObject;
    }

    private EVA vertexToEVA(Vertex evaVertex) {
        EVA eva = new EVA();
        eva.name = (String) getProperty(evaVertex, "name");
        eva.comment = (String) getProperty(evaVertex, "comment");
        eva.required = (Boolean) getProperty(evaVertex, "required");
        eva.baseClassName = (String) getProperty(evaVertex, "baseClassName");
        eva.inverseEVA = (String) getProperty(evaVertex, "inverseEVA");
        eva.cardinality = (Integer) getProperty(evaVertex, "cardinality");
        eva.distinct = (Boolean) getProperty(evaVertex, "distinct");
        eva.max = (Integer) getProperty(evaVertex, "max");
        return eva;
    }

    private Object getProperty(Vertex vertex, String name) {
        return vertex.property(name).isPresent() ? vertex.property(name).value() : null;
    }

    @Override
    public ArrayList<WDBObject> getObjects(IndexDef index, String key) throws Exception {
        return null;
    }

    private void delete(Vertex vertex) {
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        while(edges.hasNext()) {
            Edge edge = edges.next();
            Vertex inVertex = edge.inVertex();
            edge.remove();
            if(!inVertex.edges(Direction.IN).hasNext()) {
                delete(inVertex);
            }
        }
        vertex.remove();
    }

    private void deleteIfExists(TitanGraphQuery query) {
        Iterator<TitanVertex> vertices = query.vertices().iterator();
        while(vertices.hasNext()) {
            delete(vertices.next());
        }
    }
}
