/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ElementOwnership
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement ownedElement, org.omg.java.cwm.objectmodel.core.Namespace namespace )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Namespace getNamespace( org.omg.java.cwm.objectmodel.core.ModelElement ownedElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getOwnedElement( org.omg.java.cwm.objectmodel.core.Namespace namespace )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement ownedElement, org.omg.java.cwm.objectmodel.core.Namespace namespace )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement ownedElement, org.omg.java.cwm.objectmodel.core.Namespace namespace )
    throws javax.jmi.reflect.JmiException;

}
