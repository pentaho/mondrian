/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface MachineClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.Machine createMachine( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.util.List _ipAddress, java.util.List _hostName, java.lang.String _machineId )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.Machine createMachine();

}
