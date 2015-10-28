package wdb;

import wdb.metadata.ClassDef;
import wdb.metadata.IndexDef;
import wdb.metadata.WDBObject;

import java.util.ArrayList;

public interface DatabaseAdapter {
    void commit() throws Exception;
    void abort() throws Exception;
    void putClass(ClassDef classDef) throws Exception;
    ClassDef getClass(String className) throws Exception;
    void putObject(WDBObject object) throws Exception;
    ArrayList<WDBObject> getObjects(IndexDef index, String key) throws Exception;
    WDBObject getObject(String className, Integer Uid) throws Exception;
}
