/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DeployedSoftwareSystemComponents
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployedSoftwareSystem, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDeployedComponent( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployedSoftwareSystem )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDeployedSoftwareSystem( org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployedSoftwareSystem, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployedSoftwareSystem, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

}
