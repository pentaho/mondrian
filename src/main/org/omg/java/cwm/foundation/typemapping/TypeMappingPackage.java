/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface TypeMappingPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.foundation.typemapping.TypeMappingClass getTypeMapping();

  public org.omg.java.cwm.foundation.typemapping.TypeSystemClass getTypeSystem();

  public org.omg.java.cwm.foundation.typemapping.MappingTarget getMappingTarget();

  public org.omg.java.cwm.foundation.typemapping.MappingSource getMappingSource();

}
