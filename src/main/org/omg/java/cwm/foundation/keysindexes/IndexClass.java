/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.keysindexes.Index createIndex( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isPartitioning, boolean _isSorted, boolean _isUnique )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.keysindexes.Index createIndex();

}
