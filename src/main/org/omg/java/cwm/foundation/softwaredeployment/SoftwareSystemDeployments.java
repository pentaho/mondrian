/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface SoftwareSystemDeployments
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployment, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem softwareSystem )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem getSoftwareSystem( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployment )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDeployment( org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem softwareSystem )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployment, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem softwareSystem )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.DeployedSoftwareSystem deployment, org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem softwareSystem )
    throws javax.jmi.reflect.JmiException;

}
