/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface CubeRegion
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isReadOnly()
    throws javax.olap.OLAPException;

  public void setReadOnly( boolean value )
    throws javax.olap.OLAPException;

  public boolean isFullyRealized()
    throws javax.olap.OLAPException;

  public void setFullyRealized( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getMemberSelectionGroup()
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.Cube getCube()
    throws javax.olap.OLAPException;

  public void setCube( javax.olap.serversidemetadata.Cube value )
    throws javax.olap.OLAPException;

  public java.util.List getCubeDeployment()
    throws javax.olap.OLAPException;

}
