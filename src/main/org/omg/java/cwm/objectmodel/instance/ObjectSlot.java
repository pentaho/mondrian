/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface ObjectSlot
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.instance.Slot slot, org.omg.java.cwm.objectmodel.instance.Object object )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.instance.Object getObject( org.omg.java.cwm.objectmodel.instance.Slot slot )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSlot( org.omg.java.cwm.objectmodel.instance.Object object )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.instance.Slot slot, org.omg.java.cwm.objectmodel.instance.Object object )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.instance.Slot slot, org.omg.java.cwm.objectmodel.instance.Object object )
    throws javax.jmi.reflect.JmiException;

}
