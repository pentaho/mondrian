/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface DeploymentGroup
extends org.omg.java.cwm.objectmodel.core.Package {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.serversidemetadata.Schema getSchema()
    throws javax.olap.OLAPException;

  public void setSchema( javax.olap.serversidemetadata.Schema value )
    throws javax.olap.OLAPException;

  public java.util.Collection getCubeDeployment()
    throws javax.olap.OLAPException;

  public java.util.Collection getDimensionDeployment()
    throws javax.olap.OLAPException;

}
