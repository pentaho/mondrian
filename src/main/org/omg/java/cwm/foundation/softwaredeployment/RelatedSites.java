/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface RelatedSites
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.softwaredeployment.Site containingSite, org.omg.java.cwm.foundation.softwaredeployment.Site containedSite )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getContainedSite( org.omg.java.cwm.foundation.softwaredeployment.Site containingSite )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getContainingSite( org.omg.java.cwm.foundation.softwaredeployment.Site containedSite )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.softwaredeployment.Site containingSite, org.omg.java.cwm.foundation.softwaredeployment.Site containedSite )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.softwaredeployment.Site containingSite, org.omg.java.cwm.foundation.softwaredeployment.Site containedSite )
    throws javax.jmi.reflect.JmiException;

}
