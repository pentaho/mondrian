/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface KeyRelationshipFeatures
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getKeyRelationship( org.omg.java.cwm.objectmodel.core.StructuralFeature feature )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getFeature( org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship )
    throws javax.jmi.reflect.JmiException;

}
