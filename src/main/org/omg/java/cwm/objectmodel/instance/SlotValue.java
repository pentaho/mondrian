/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface SlotValue
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.instance.Slot valueSlot, org.omg.java.cwm.objectmodel.instance.Instance value )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.instance.Instance getValue( org.omg.java.cwm.objectmodel.instance.Slot valueSlot )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getValueSlot( org.omg.java.cwm.objectmodel.instance.Instance value )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.instance.Slot valueSlot, org.omg.java.cwm.objectmodel.instance.Instance value )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.instance.Slot valueSlot, org.omg.java.cwm.objectmodel.instance.Instance value )
    throws javax.jmi.reflect.JmiException;

}
