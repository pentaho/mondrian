/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface CubeDeployment
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.serversidemetadata.CubeRegion getCubeRegion()
    throws javax.olap.OLAPException;

  public void setCubeRegion( javax.olap.serversidemetadata.CubeRegion value )
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.DeploymentGroup getDeploymentGroup()
    throws javax.olap.OLAPException;

  public void setDeploymentGroup( javax.olap.serversidemetadata.DeploymentGroup value )
    throws javax.olap.OLAPException;

  public java.util.Collection getContentMap()
    throws javax.olap.OLAPException;

}
