/* Generated By:JJTree: Do not edit this line. True.java */

package wdb.parser;

import wdb.DatabaseAdapter;
import wdb.metadata.IndexSelectResult;
import wdb.metadata.WDBObject;

import java.util.ArrayList;

public class True extends SimpleNode {
  public True(int id) {
    super(id);
  }

  public True(QueryParser p, int id) {
    super(p, id);
  }
  public IndexSelectResult filterObjectsWithIndexes(DatabaseAdapter da, ArrayList indexes) throws Exception
  {
    IndexSelectResult isr = new IndexSelectResult();
    //These conditions are not supported so return a "scan" or "can't help" result
    return isr;
  }
  public boolean eval(DatabaseAdapter da, WDBObject wdbO)
  {
    return true;
  }

}
