/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface StructuralFeatureType
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.StructuralFeature structuralFeature, org.omg.java.cwm.objectmodel.core.Classifier type )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getType( org.omg.java.cwm.objectmodel.core.StructuralFeature structuralFeature )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getStructuralFeature( org.omg.java.cwm.objectmodel.core.Classifier type )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.StructuralFeature structuralFeature, org.omg.java.cwm.objectmodel.core.Classifier type )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.StructuralFeature structuralFeature, org.omg.java.cwm.objectmodel.core.Classifier type )
    throws javax.jmi.reflect.JmiException;

}
