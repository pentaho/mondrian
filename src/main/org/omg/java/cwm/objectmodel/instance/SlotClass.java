/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.instance;



public interface SlotClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.instance.Slot createSlot( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.instance.Slot createSlot();

}
