/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface ModelElementResponsibility
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getResponsibleParty( org.omg.java.cwm.objectmodel.core.ModelElement modelElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getModelElement( org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

}
