/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface UnionClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.Union createUnion( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.Union createUnion();

}
