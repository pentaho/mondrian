/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface Machine
extends org.omg.java.cwm.objectmodel.core.Namespace {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.util.List getIpAddress();

  public java.util.List getHostName();

  public java.lang.String getMachineId();

  public void setMachineId( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.softwaredeployment.Site getSite();

  public void setSite( org.omg.java.cwm.foundation.softwaredeployment.Site value );

  public java.util.Collection getDeployedComponent();

}
