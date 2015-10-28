/*
 * Created on Feb 16, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package wdb;

import wdb.metadata.*;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;

import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.SecondaryCursor;

import java.util.*;
/**
 * @author Bo Li
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class SleepyCatDataAdapter implements DatabaseAdapter {

	private Transaction txn;
	private SleepyCatDataBase scdb;
	
	public SleepyCatDataAdapter(SleepyCatDataBase scdb, Transaction txn)
	{
		this.scdb = scdb;
		this.txn = txn;
	}

	public void commit() throws Exception
	{
		this.txn.commit();
	}
	
	public void abort() throws Exception
	{
		this.txn.abort();
	}
	public void putClass(ClassDef classDef) throws Exception
	{
		EntryBinding keyBinding = new SerialBinding(this.scdb.getClassCatalog(), String.class);
		EntryBinding dataBinding = new SerialBinding(this.scdb.getClassCatalog(), ClassDef.class);
		
		DatabaseEntry theKey = new DatabaseEntry();
		keyBinding.objectToEntry(makeClassKey(classDef.name), theKey);
		
		DatabaseEntry theData = new DatabaseEntry();
		dataBinding.objectToEntry(classDef, theData);
		
		this.scdb.getClassDb().put(this.txn, theKey, theData);
	}
	
	public ClassDef getClass(String className) throws Exception
	{
		EntryBinding keyBinding = new SerialBinding(this.scdb.getClassCatalog(), String.class);
		EntryBinding dataBinding = new SerialBinding(this.scdb.getClassCatalog(), ClassDef.class);
		
		DatabaseEntry theKey = new DatabaseEntry();
		keyBinding.objectToEntry(makeClassKey(className), theKey);
		
	    DatabaseEntry theData = new DatabaseEntry();
	    
	    OperationStatus status;
	    status = this.scdb.getClassDb().get(this.txn, theKey, theData, LockMode.DEFAULT);
	    
	    if(status == OperationStatus.NOTFOUND)
	    {
	    	throw new ClassNotFoundException("Class \"" + className + "\" is not defined");
	    }
	    	
	    return (ClassDef)dataBinding.entryToObject(theData);
	}
	
	public void putObject(WDBObject object) throws Exception
	{
		EntryBinding keyBinding = new SerialBinding(this.scdb.getClassCatalog(), String.class);
		EntryBinding dataBinding = new SerialBinding(this.scdb.getClassCatalog(), WDBObject.class);
		
		DatabaseEntry theKey = new DatabaseEntry();
		keyBinding.objectToEntry(makeObjectKey(object.getClassName(), object.getUid()), theKey);
		
		DatabaseEntry theData = new DatabaseEntry();
		dataBinding.objectToEntry(object, theData);
		
		this.scdb.getObjectDb().put(null, theKey, theData);
	}
	
	public ArrayList<WDBObject> getObjects(IndexDef index, String key) throws Exception
	{
		EntryBinding keyBinding = new SerialBinding(this.scdb.getClassCatalog(), String.class);
		EntryBinding dataBinding = new SerialBinding(this.scdb.getClassCatalog(), WDBObject.class);
		
		DatabaseEntry theKey = new DatabaseEntry();
		keyBinding.objectToEntry(key, theKey);
		
	    DatabaseEntry theData = new DatabaseEntry();
	    
	    SecondaryCursor cursor = this.scdb.getSecDb(index).openSecondaryCursor(this.txn, null);
	    
	    OperationStatus status;
	    status = cursor.getSearchKey(theKey, theData, LockMode.DEFAULT);
	    
	    ArrayList<WDBObject> foundObjectsList = new ArrayList<WDBObject>();
	    while(status == OperationStatus.SUCCESS)
	    {
	    	foundObjectsList.add((WDBObject)dataBinding.entryToObject(theData));
	    	status = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
	    }
	    
	    cursor.close();
	    return foundObjectsList;
	}
	
	public WDBObject getObject(String className, Integer Uid) throws Exception
	{
		EntryBinding keyBinding = new SerialBinding(this.scdb.getClassCatalog(), String.class);
		EntryBinding dataBinding = new SerialBinding(this.scdb.getClassCatalog(), WDBObject.class);
		
		DatabaseEntry theKey = new DatabaseEntry();
		keyBinding.objectToEntry(makeObjectKey(className, Uid), theKey);
		
	    DatabaseEntry theData = new DatabaseEntry();
	    
	    OperationStatus status;
	    status = this.scdb.getObjectDb().get(null, theKey, theData, LockMode.DEFAULT);
	    
	    if(status == OperationStatus.NOTFOUND)
	    {
	    	throw new Exception("Object with UID " + Uid.toString() + " of class \"" + className + "\" does not exist");
	    }
	    	
	    return (WDBObject)dataBinding.entryToObject(theData);
	}
	
	private String makeClassKey(String className)
	{
		return this.scdb.getClassKeyPrefix()+":"+className;
	}
	
	private String makeObjectKey(String className, Integer Uid)
	{
		return this.scdb.getObjectKeyPrefix()+":"+Uid.toString();
	}
}
