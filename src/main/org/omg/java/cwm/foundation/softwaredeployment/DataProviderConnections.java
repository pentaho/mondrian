/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DataProviderConnections
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.DataProvider dataProvider, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection resourceConnection )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getResourceConnection( org.omg.java.cwm.foundation.softwaredeployment.DataProvider dataProvider )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.DataProvider getDataProvider( org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection resourceConnection )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.DataProvider dataProvider, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection resourceConnection )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.DataProvider dataProvider, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection resourceConnection )
    throws javax.jmi.reflect.JmiException;

}
