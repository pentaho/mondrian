/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface TypeMappingClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.typemapping.TypeMapping createTypeMapping( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isBestMatch, boolean _isLossy )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.typemapping.TypeMapping createTypeMapping();

}
