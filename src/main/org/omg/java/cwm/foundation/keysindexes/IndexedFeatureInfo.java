/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexedFeatureInfo
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.keysindexes.Index index, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getIndexedFeature( org.omg.java.cwm.foundation.keysindexes.Index index )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.keysindexes.Index getIndex( org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.keysindexes.Index index, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.keysindexes.Index index, org.omg.java.cwm.foundation.keysindexes.IndexedFeature indexedFeature )
    throws javax.jmi.reflect.JmiException;

}
