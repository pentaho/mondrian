/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DataManagerDataPackage
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Package dataPackage, org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDataManager( org.omg.java.cwm.objectmodel.core.Package dataPackage )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getDataPackage( org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Package dataPackage, org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Package dataPackage, org.omg.java.cwm.foundation.softwaredeployment.DataManager dataManager )
    throws javax.jmi.reflect.JmiException;

}
