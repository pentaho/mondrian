/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface PackageUsageClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.PackageUsage createPackageUsage( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _kind, java.lang.String _packageAlias )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.PackageUsage createPackageUsage();

}
