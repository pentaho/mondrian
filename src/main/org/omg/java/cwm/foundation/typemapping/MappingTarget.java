/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface MappingTarget
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier targetType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingTo )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getMappingTo( org.omg.java.cwm.objectmodel.core.Classifier targetType )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getTargetType( org.omg.java.cwm.foundation.typemapping.TypeMapping mappingTo )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier targetType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingTo )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier targetType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingTo )
    throws javax.jmi.reflect.JmiException;

}
