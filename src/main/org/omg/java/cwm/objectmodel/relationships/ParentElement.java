/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.relationships;



public interface ParentElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier parent, org.omg.java.cwm.objectmodel.relationships.Generalization specialization )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSpecialization( org.omg.java.cwm.objectmodel.core.Classifier parent )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getParent( org.omg.java.cwm.objectmodel.relationships.Generalization specialization )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier parent, org.omg.java.cwm.objectmodel.relationships.Generalization specialization )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier parent, org.omg.java.cwm.objectmodel.relationships.Generalization specialization )
    throws javax.jmi.reflect.JmiException;

}
