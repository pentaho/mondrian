/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface DataObjectSetClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.analysis.transformation.DataObjectSet createDataObjectSet( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.DataObjectSet createDataObjectSet();

}
