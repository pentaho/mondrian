/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface UniqueKeyRelationship
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship, org.omg.java.cwm.foundation.keysindexes.UniqueKey uniqueKey )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.keysindexes.UniqueKey getUniqueKey( org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getKeyRelationship( org.omg.java.cwm.foundation.keysindexes.UniqueKey uniqueKey )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship, org.omg.java.cwm.foundation.keysindexes.UniqueKey uniqueKey )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.keysindexes.KeyRelationship keyRelationship, org.omg.java.cwm.foundation.keysindexes.UniqueKey uniqueKey )
    throws javax.jmi.reflect.JmiException;

}
