/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface ProviderConnection
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isReadOnly();

  public void setReadOnly( boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.softwaredeployment.DataProvider getDataProvider();

  public void setDataProvider( org.omg.java.cwm.foundation.softwaredeployment.DataProvider value );

  public org.omg.java.cwm.foundation.softwaredeployment.DataManager getDataManager();

  public void setDataManager( org.omg.java.cwm.foundation.softwaredeployment.DataManager value );

}
