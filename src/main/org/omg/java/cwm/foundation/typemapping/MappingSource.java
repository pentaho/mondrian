/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface MappingSource
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier sourceType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingFrom )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getMappingFrom( org.omg.java.cwm.objectmodel.core.Classifier sourceType )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getSourceType( org.omg.java.cwm.foundation.typemapping.TypeMapping mappingFrom )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier sourceType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingFrom )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier sourceType, org.omg.java.cwm.foundation.typemapping.TypeMapping mappingFrom )
    throws javax.jmi.reflect.JmiException;

}
