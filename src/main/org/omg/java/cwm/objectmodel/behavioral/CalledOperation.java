/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface CalledOperation
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.behavioral.CallAction callAction, org.omg.java.cwm.objectmodel.behavioral.Operation operation )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.Operation getOperation( org.omg.java.cwm.objectmodel.behavioral.CallAction callAction )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getCallAction( org.omg.java.cwm.objectmodel.behavioral.Operation operation )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.behavioral.CallAction callAction, org.omg.java.cwm.objectmodel.behavioral.Operation operation )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.behavioral.CallAction callAction, org.omg.java.cwm.objectmodel.behavioral.Operation operation )
    throws javax.jmi.reflect.JmiException;

}
