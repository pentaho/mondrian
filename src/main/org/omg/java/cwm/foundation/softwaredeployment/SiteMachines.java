/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface SiteMachines
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.Site site, org.omg.java.cwm.foundation.softwaredeployment.Machine machine )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getMachine( org.omg.java.cwm.foundation.softwaredeployment.Site site )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.Site getSite( org.omg.java.cwm.foundation.softwaredeployment.Machine machine )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.Site site, org.omg.java.cwm.foundation.softwaredeployment.Machine machine )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.Site site, org.omg.java.cwm.foundation.softwaredeployment.Machine machine )
    throws javax.jmi.reflect.JmiException;

}
