/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface DocumentDescribes
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.Document document )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDocument( org.omg.java.cwm.objectmodel.core.ModelElement modelElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getModelElement( org.omg.java.cwm.foundation.businessinformation.Document document )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.Document document )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.Document document )
    throws javax.jmi.reflect.JmiException;

}
