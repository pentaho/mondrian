/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexSpansClass
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.CoreClass spannedClass, org.omg.java.cwm.foundation.keysindexes.Index index )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getIndex( org.omg.java.cwm.objectmodel.core.CoreClass spannedClass )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.CoreClass getSpannedClass( org.omg.java.cwm.foundation.keysindexes.Index index )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.CoreClass spannedClass, org.omg.java.cwm.foundation.keysindexes.Index index )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.CoreClass spannedClass, org.omg.java.cwm.foundation.keysindexes.Index index )
    throws javax.jmi.reflect.JmiException;

}
