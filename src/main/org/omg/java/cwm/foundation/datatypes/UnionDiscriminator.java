/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface UnionDiscriminator
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.StructuralFeature discriminator, org.omg.java.cwm.foundation.datatypes.Union discriminatedUnion )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDiscriminatedUnion( org.omg.java.cwm.objectmodel.core.StructuralFeature discriminator )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.StructuralFeature getDiscriminator( org.omg.java.cwm.foundation.datatypes.Union discriminatedUnion )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.StructuralFeature discriminator, org.omg.java.cwm.foundation.datatypes.Union discriminatedUnion )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.StructuralFeature discriminator, org.omg.java.cwm.foundation.datatypes.Union discriminatedUnion )
    throws javax.jmi.reflect.JmiException;

}
