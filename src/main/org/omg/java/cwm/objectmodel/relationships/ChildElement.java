/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.relationships;



public interface ChildElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier child, org.omg.java.cwm.objectmodel.relationships.Generalization generalization )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getGeneralization( org.omg.java.cwm.objectmodel.core.Classifier child )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getChild( org.omg.java.cwm.objectmodel.relationships.Generalization generalization )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier child, org.omg.java.cwm.objectmodel.relationships.Generalization generalization )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier child, org.omg.java.cwm.objectmodel.relationships.Generalization generalization )
    throws javax.jmi.reflect.JmiException;

}
