/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface SystemTypespace
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.typemapping.TypeSystem typespace, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem supportingSystem )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getSupportingSystem( org.omg.java.cwm.foundation.typemapping.TypeSystem typespace )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTypespace( org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem supportingSystem )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.typemapping.TypeSystem typespace, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem supportingSystem )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.typemapping.TypeSystem typespace, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem supportingSystem )
    throws javax.jmi.reflect.JmiException;

}
