/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface DataObjectSetElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement element, org.omg.java.cwm.analysis.transformation.DataObjectSet set )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSet( org.omg.java.cwm.objectmodel.core.ModelElement element )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getElement( org.omg.java.cwm.analysis.transformation.DataObjectSet set )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement element, org.omg.java.cwm.analysis.transformation.DataObjectSet set )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement element, org.omg.java.cwm.analysis.transformation.DataObjectSet set )
    throws javax.jmi.reflect.JmiException;

}
