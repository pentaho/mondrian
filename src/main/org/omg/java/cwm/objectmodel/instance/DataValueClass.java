/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface DataValueClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.instance.DataValue createDataValue( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _value )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.instance.DataValue createDataValue();

}
