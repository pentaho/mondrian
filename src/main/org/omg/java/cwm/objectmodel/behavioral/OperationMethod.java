/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface OperationMethod
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.behavioral.Operation specification, org.omg.java.cwm.objectmodel.behavioral.Method method )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getMethod( org.omg.java.cwm.objectmodel.behavioral.Operation specification )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.Operation getSpecification( org.omg.java.cwm.objectmodel.behavioral.Method method )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.behavioral.Operation specification, org.omg.java.cwm.objectmodel.behavioral.Method method )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.behavioral.Operation specification, org.omg.java.cwm.objectmodel.behavioral.Method method )
    throws javax.jmi.reflect.JmiException;

}
