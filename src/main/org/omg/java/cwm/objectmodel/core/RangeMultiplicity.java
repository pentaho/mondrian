/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface RangeMultiplicity
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Multiplicity multiplicity, org.omg.java.cwm.objectmodel.core.MultiplicityRange range )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getRange( org.omg.java.cwm.objectmodel.core.Multiplicity multiplicity )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Multiplicity getMultiplicity( org.omg.java.cwm.objectmodel.core.MultiplicityRange range )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Multiplicity multiplicity, org.omg.java.cwm.objectmodel.core.MultiplicityRange range )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Multiplicity multiplicity, org.omg.java.cwm.objectmodel.core.MultiplicityRange range )
    throws javax.jmi.reflect.JmiException;

}
