/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface EnumerationLiterals
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.datatypes.Enumeration enumeration, org.omg.java.cwm.foundation.datatypes.EnumerationLiteral literal )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getLiteral( org.omg.java.cwm.foundation.datatypes.Enumeration enumeration )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.Enumeration getEnumeration( org.omg.java.cwm.foundation.datatypes.EnumerationLiteral literal )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.datatypes.Enumeration enumeration, org.omg.java.cwm.foundation.datatypes.EnumerationLiteral literal )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.datatypes.Enumeration enumeration, org.omg.java.cwm.foundation.datatypes.EnumerationLiteral literal )
    throws javax.jmi.reflect.JmiException;

}
