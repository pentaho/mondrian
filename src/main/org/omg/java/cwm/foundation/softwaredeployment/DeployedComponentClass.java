/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DeployedComponentClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent createDeployedComponent( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _pathname )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent createDeployedComponent();

}
