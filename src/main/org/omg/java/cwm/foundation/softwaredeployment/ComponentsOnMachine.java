/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface ComponentsOnMachine
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.Machine machine, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDeployedComponent( org.omg.java.cwm.foundation.softwaredeployment.Machine machine )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.Machine getMachine( org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.Machine machine, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.Machine machine, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployedComponent )
    throws javax.jmi.reflect.JmiException;

}
