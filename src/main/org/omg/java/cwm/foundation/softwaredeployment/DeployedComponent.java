/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DeployedComponent
extends org.omg.java.cwm.objectmodel.core.Package {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.String getPathname();

  public void setPathname( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.softwaredeployment.Component getComponent();

  public void setComponent( org.omg.java.cwm.foundation.softwaredeployment.Component value );

  public org.omg.java.cwm.foundation.softwaredeployment.Machine getMachine();

  public void setMachine( org.omg.java.cwm.foundation.softwaredeployment.Machine value );

}
