/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface TypeAliasClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.TypeAlias createTypeAlias( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.TypeAlias createTypeAlias();

}
