/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface EnumerationLiteralClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.EnumerationLiteral createEnumerationLiteral( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.Expression _value )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.EnumerationLiteral createEnumerationLiteral();

}
