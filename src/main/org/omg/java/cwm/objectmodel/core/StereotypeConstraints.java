/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface StereotypeConstraints
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Constraint stereotypeConstraint, org.omg.java.cwm.objectmodel.core.Stereotype constrainedStereotype )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Stereotype getConstrainedStereotype( org.omg.java.cwm.objectmodel.core.Constraint stereotypeConstraint )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getStereotypeConstraint( org.omg.java.cwm.objectmodel.core.Stereotype constrainedStereotype )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Constraint stereotypeConstraint, org.omg.java.cwm.objectmodel.core.Stereotype constrainedStereotype )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Constraint stereotypeConstraint, org.omg.java.cwm.objectmodel.core.Stereotype constrainedStereotype )
    throws javax.jmi.reflect.JmiException;

}
