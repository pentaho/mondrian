/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ElementConstraint
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement constrainedElement, org.omg.java.cwm.objectmodel.core.Constraint constraint )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getConstraint( org.omg.java.cwm.objectmodel.core.ModelElement constrainedElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getConstrainedElement( org.omg.java.cwm.objectmodel.core.Constraint constraint )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement constrainedElement, org.omg.java.cwm.objectmodel.core.Constraint constraint )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement constrainedElement, org.omg.java.cwm.objectmodel.core.Constraint constraint )
    throws javax.jmi.reflect.JmiException;

}
