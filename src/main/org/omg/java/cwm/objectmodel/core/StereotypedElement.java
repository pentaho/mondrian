/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface StereotypedElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement extendedElement, org.omg.java.cwm.objectmodel.core.Stereotype stereotype )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Stereotype getStereotype( org.omg.java.cwm.objectmodel.core.ModelElement extendedElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getExtendedElement( org.omg.java.cwm.objectmodel.core.Stereotype stereotype )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement extendedElement, org.omg.java.cwm.objectmodel.core.Stereotype stereotype )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement extendedElement, org.omg.java.cwm.objectmodel.core.Stereotype stereotype )
    throws javax.jmi.reflect.JmiException;

}
