/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface TypeSystemClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.typemapping.TypeSystem createTypeSystem( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _version )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.typemapping.TypeSystem createTypeSystem();

}
