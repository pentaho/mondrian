/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface CallArguments
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.behavioral.Argument actualArgument, org.omg.java.cwm.objectmodel.behavioral.CallAction callAction )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.behavioral.CallAction getCallAction( org.omg.java.cwm.objectmodel.behavioral.Argument actualArgument )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getActualArgument( org.omg.java.cwm.objectmodel.behavioral.CallAction callAction )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.behavioral.Argument actualArgument, org.omg.java.cwm.objectmodel.behavioral.CallAction callAction )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.behavioral.Argument actualArgument, org.omg.java.cwm.objectmodel.behavioral.CallAction callAction )
    throws javax.jmi.reflect.JmiException;

}
