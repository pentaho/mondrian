/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface ParameterClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.behavioral.Parameter createParameter( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.Expression _defaultValue, org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKind _kind )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.Parameter createParameter();

}
