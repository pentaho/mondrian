/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DataManagerConnections
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection clientConnection )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getClientConnection( org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.DataManager getDataManager( org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection clientConnection )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection clientConnection )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager, org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection clientConnection )
    throws javax.jmi.reflect.JmiException;

}
