/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexedFeatures
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getIndexedFeature( org.omg.java.cwm.objectmodel.core.StructuralFeature feature )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.StructuralFeature getFeature( org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.StructuralFeature feature, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

}
