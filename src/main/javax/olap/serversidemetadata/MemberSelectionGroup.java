/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface MemberSelectionGroup
extends org.omg.java.cwm.objectmodel.core.CoreClass {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getMemberSelection()
    throws javax.olap.OLAPException;

  public javax.olap.serversidemetadata.CubeRegion getCubeRegion()
    throws javax.olap.OLAPException;

  public void setCubeRegion( javax.olap.serversidemetadata.CubeRegion value )
    throws javax.olap.OLAPException;

}
