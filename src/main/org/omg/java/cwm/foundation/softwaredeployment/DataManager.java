/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DataManager
extends org.omg.java.cwm.foundation.softwaredeployment.DeployedComponent {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isCaseSensitive();

  public void setCaseSensitive( boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getDataPackage();

}
