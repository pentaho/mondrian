/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface ComponentDeployments
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.Component component, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployment )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDeployment( org.omg.java.cwm.foundation.softwaredeployment.Component component )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.Component getComponent( org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployment )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.Component component, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployment )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.Component component, org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent deployment )
    throws javax.jmi.reflect.JmiException;

}
