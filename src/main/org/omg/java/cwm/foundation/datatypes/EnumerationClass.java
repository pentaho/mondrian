/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface EnumerationClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.Enumeration createEnumeration( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract, boolean _isOrdered )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.Enumeration createEnumeration();

}
