
package wdb;

import wdb.metadata.*;
import wdb.parser.*;

import java.io.*;
import java.util.*;

/**
 * @author Bo Li
 */
public class WDB {

	private static QueryParser parser;
	private static DatabaseTool db;
	private static BufferedReader in;

	public static void main(String[] args) {
		String installRoot = System.getenv("INSTANCE_ROOT");
		if (installRoot == null) {
			System.out.println("Please set INSTANCE_ROOT variable");
		}
		File installRootDir = new File(installRoot);
		if (!installRootDir.exists() || !installRootDir.isDirectory()) {
			System.out.println("INSTANCE_ROOT variable is not a valid directory");
		}
		File dbDir = new File(installRootDir, "thedb");
		if (!dbDir.exists()) {
			dbDir.mkdir();
		}

		try {
			if(args.length > 0 && "sleepycat".equals(args[0])) {
				db = new SleepyCatDataBase(dbDir.toString());
				db.openDb("test");
			}
			else {
				db = new TitanDatabase();
				db.openDb(dbDir.toString());
			}

			System.out.println("WDB Simantic Database Project");
			System.out.println("Copyright 2006 University of Texas at Austin");

			WDB.in = new BufferedReader(new InputStreamReader(System.in));
			WDB.parser = new QueryParser(WDB.in);
			Query q;

			while (true) {
				try {
					if (!in.ready()) {
						System.out.print("\nWDB>");
					}

					q = parser.getNextQuery();
					if (q == null) {
						break;
					} else {
						processQuery(q);
					}
				} catch (ParseException pe) {
					System.out.println("SYNTAX ERROR: " + pe.getMessage());
					QueryParser.ReInit(System.in);
				} catch (TokenMgrError tme) {
					System.out.println("PARSER ERROR: " + tme.getMessage());
					break;
				} catch (IOException ioe) {
					System.out.println("STANDARD IN ERROR: " + ioe.getMessage());
					break;
				} catch (NumberFormatException nfe) {
					System.out.println("PARSE ERROR: Failed to convert to Integer " + nfe.getMessage());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getStackTrace());
		} finally {
			try {
				db.closeDb();
			} catch (Exception e) {
				System.out.println("DATABASE CLOSE ERROR: " + e.getMessage());
			}
		}
	}


	static private void processQuery(Query q) {
		if (q.getClass() == SourceQuery.class) {
			SourceQuery sq = (SourceQuery) q;

			try {
				QueryParser.ReInit(new FileReader(sq.filename));
				Query fq;
				while (true) {
					fq = parser.getNextQuery();
					if (fq == null) {
						break;
					} else {
						processQuery(fq);
					}
				}
			} catch (FileNotFoundException e) {
				System.out.println("FILE OPEN ERROR: " + e.getMessage());
			} catch (ParseException pe) {
				System.out.println("SYNTAX ERROR: " + pe.getMessage());
			} catch (TokenMgrError tme) {
				System.out.println("PARSER ERROR: " + tme.getMessage());
			} finally {
				QueryParser.ReInit(WDB.in);
			}
		}

		if (q.getClass() == ClassDef.class || q.getClass() == SubclassDef.class) {
			ClassDef cd = (ClassDef) q;

			try {
				DatabaseAdapter da = db.newTransaction();

				try {
					try {
						da.getClass(cd.name);
						//That class alreadly exists;
						throw new Exception("Class \"" + cd.name + "\" alreadly exists");
					} catch (ClassNotFoundException cnfe) {
						if (cd.getClass() == SubclassDef.class) {
							ClassDef baseClass = null;
							for (int i = 0; i < ((SubclassDef) cd).numberOfSuperClasses(); i++) {
								//Cycles are implisitly checked since getClass will fail for the current defining class
								ClassDef superClass = da.getClass(((SubclassDef) cd).getSuperClass(i));
								if (baseClass == null) {
									baseClass = superClass.getBaseClass(da);
								} else if (!baseClass.name.equals(superClass.getBaseClass(da).name)) {
									throw new Exception("Super classes of class \"" + cd.name + "\" does not share the same base class");
								}
							}
						}

						da.putClass(cd);
						da.commit();
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.toString() + ": " + e.getStackTrace());
					da.abort();
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString() + ": " + e.getStackTrace());
			}
		}

		if (q.getClass() == ModifyQuery.class) {
			ModifyQuery mq = (ModifyQuery) q;
			try {
				DatabaseAdapter da = db.newTransaction();
				try {
					ClassDef targetClass = da.getClass(mq.className);
					WDBObject[] targetClassObjs = targetClass.search(mq.expression, da);
					if (mq.limit > -1 && targetClassObjs.length > mq.limit) {
						throw new Exception("Matching entities exceeds limit of " + mq.limit.toString());
					}
					for (int i = 0; i < targetClassObjs.length; i++) {
						setValues(mq.assignmentList, targetClassObjs[i], da);
					}
					da.commit();
				} catch (Exception e) {
					e.printStackTrace();
					da.abort();
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString());
			}
		}

		if (q.getClass() == InsertQuery.class) {
			InsertQuery iq = (InsertQuery) q;

			try {
				DatabaseAdapter da = db.newTransaction();

				try {
					ClassDef targetClass = da.getClass(iq.className);
					WDBObject newObject = null;

					if (iq.fromClassName != null) {
						//Inserting from an entity of a superclass...
						if (targetClass.getClass() == SubclassDef.class) {
							SubclassDef targetSubClass = (SubclassDef) targetClass;
							ClassDef fromClass = da.getClass(iq.fromClassName);
							if (targetSubClass.isSubclassOf(fromClass.name, da)) {
								WDBObject[] fromObjects = fromClass.search(iq.expression, da);
								if (fromObjects.length <= 0) {
									throw new IllegalStateException("Can't find any entities from class \"" + fromClass.name + "\" to extend");
								}
								for (int i = 0; i < fromObjects.length; i++) {
									newObject = targetSubClass.newInstance(fromObjects[i].getBaseObject(da), da);
									setValues(iq.assignmentList, newObject, da);
								}
							} else {
								throw new IllegalStateException("Inserted class \"" + targetClass.name + "\" is not a subclass of the from class \"" + iq.fromClassName);
							}
						} else {
							throw new IllegalStateException("Can't extend base class \"" + targetClass.name + "\" from class \"" + iq.fromClassName);
						}
					} else {
						//Just inserting a new entity
						newObject = targetClass.newInstance(null, da);
						setDefaultValues(targetClass, newObject, da);
						setValues(iq.assignmentList, newObject, da);
						checkRequiredValues(targetClass, newObject, da);
					}

					if (newObject != null) {
						newObject.commit(da);
					}
					da.commit();
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.toString());
					da.abort();
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString());
			}
		}
		if (q.getClass() == IndexDef.class) {
			IndexDef indexQ = (IndexDef) q;

			try {
				DatabaseAdapter da = db.newTransaction();
				try {
					ClassDef classDef = da.getClass(indexQ.className);
					classDef.addIndex(indexQ, da);

					db.openSecDb(indexQ);

					da.commit();
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.toString());
					da.abort();
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString());
			}
		}
		if (q.getClass() == RetrieveQuery.class) {
			//Ok, its a retrieve...
			RetrieveQuery rq = (RetrieveQuery) q;

			try {


				DatabaseAdapter da = db.newTransaction();
				try {
					ClassDef targetClass = da.getClass(rq.className);
					WDBObject[] targetClassObjs = targetClass.search(rq.expression, da);

					int i, j;
					String[][] table;
					String[][] newtable;

					PrintNode node = new PrintNode(0, 0);
					for (j = 0; j < rq.numAttributePaths(); j++) {
						targetClass.printAttributeName(node, rq.getAttributePath(j), da);
					}
					table = node.printRow();
					for (i = 0; i < targetClassObjs.length; i++) {
						node = new PrintNode(0, 0);
						for (j = 0; j < rq.numAttributePaths(); j++) {
							targetClassObjs[i].PrintAttribute(node, rq.getAttributePath(j), da);
						}
						newtable = joinRows(table, node.printRow());
						table = newtable;
					}

					da.commit();

					Integer[] columnWidths = new Integer[table[0].length];

					for (i = 0; i < columnWidths.length; i++) {
						columnWidths[i] = 0;
						for (j = 0; j < table.length; j++) {
							if (i < table[j].length && table[j][i] != null && table[j][i].length() > columnWidths[i]) {
								columnWidths[i] = table[j][i].length();
							}
						}
					}

					for (i = 0; i < table.length; i++) {
						for (j = 0; j < table[0].length; j++) {
							if (j >= table[i].length || table[i][j] == null) {
								System.out.format("| %" + columnWidths[j].toString() + "s ", "");
							} else {
								System.out.format("| %" + columnWidths[j].toString() + "s ", table[i][j]);
							}
						}
						System.out.format("|%n");
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.toString());
					da.abort();
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.toString());
			}
		}
	}

	private static void setDefaultValues(ClassDef targetClass, WDBObject targetObject, DatabaseAdapter scda) throws Exception {
		for (int j = 0; j < targetClass.numberOfAttributes(); j++) {
			if (targetClass.getAttribute(j) instanceof DVA) {
				DVA dva = (DVA) targetClass.getAttribute(j);
				if (dva.initialValue != null) {
					targetObject.setDvaValue(dva.name, dva.initialValue, scda);
				}
			}
		}
	}

	private static void checkRequiredValues(ClassDef targetClass, WDBObject targetObject, DatabaseAdapter scda) throws Exception {
		for (int j = 0; j < targetClass.numberOfAttributes(); j++) {
			Attribute attribute = (Attribute) targetClass.getAttribute(j);
			if (attribute.required != null && attribute.required && targetObject.getDvaValue(attribute.name, scda) == null) {
				throw new Exception("Attribute \"" + targetClass.getAttribute(j).name + "\" is required");
			}
		}
	}

	private static void setValues(ArrayList assignmentList, WDBObject targetObject, DatabaseAdapter scda) throws Exception {
		for (int j = 0; j < assignmentList.size(); j++) {
			if (assignmentList.get(j) instanceof DvaAssignment) {
				DvaAssignment dvaAssignment = (DvaAssignment) assignmentList.get(j);
				targetObject.setDvaValue(dvaAssignment.AttributeName, dvaAssignment.Value, scda);
			} else if (assignmentList.get(j) instanceof EvaAssignment) {
				EvaAssignment evaAssignment = (EvaAssignment) assignmentList.get(j);
				if (evaAssignment.mode == EvaAssignment.REPLACE_MODE) {
					WDBObject[] currentObjects = targetObject.getEvaObjects(evaAssignment.AttributeName, scda);
					targetObject.removeEvaObjects(evaAssignment.AttributeName, evaAssignment.targetClass, currentObjects, scda);
					targetObject.addEvaObjects(evaAssignment.AttributeName, evaAssignment.targetClass, evaAssignment.expression, scda);
				} else if (evaAssignment.mode == EvaAssignment.EXCLUDE_MODE) {
					targetObject.removeEvaObjects(evaAssignment.AttributeName, evaAssignment.targetClass, evaAssignment.expression, scda);
				} else if (evaAssignment.mode == EvaAssignment.INCLUDE_MODE) {
					targetObject.addEvaObjects(evaAssignment.AttributeName, evaAssignment.targetClass, evaAssignment.expression, scda);
				} else {
					throw new Exception("Unsupported multivalue EVA insert/modify mode");
				}
			}
		}
	}

	private static String[][] joinRows(String[][] row1, String[][] row2) {
		if (row1.length <= 0) {
			return row2;
		} else {
			String[][] newRow = new String[row1.length + row2.length][row1[0].length];
			int i, j;
			for (i = 0; i < row1.length; i++) {
				newRow[i] = row1[i];
			}
			for (j = i; j < row2.length + i; j++) {
				newRow[j] = row2[j - i];
			}

			return newRow;
		}
	}
}
