/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface UniqueKeyClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.keysindexes.UniqueKey createUniqueKey( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.keysindexes.UniqueKey createUniqueKey();

}
