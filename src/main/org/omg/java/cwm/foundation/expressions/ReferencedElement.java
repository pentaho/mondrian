/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.expressions;



public interface ReferencedElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.expressions.ElementNode elementNode )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getElementNode( org.omg.java.cwm.objectmodel.core.ModelElement modelElement )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.ModelElement getModelElement( org.omg.java.cwm.foundation.expressions.ElementNode elementNode )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.expressions.ElementNode elementNode )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.expressions.ElementNode elementNode )
    throws javax.jmi.reflect.JmiException;

}
