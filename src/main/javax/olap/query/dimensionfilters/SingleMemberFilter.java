/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface SingleMemberFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Member getMember()
    throws javax.olap.OLAPException;

  public void setMember( javax.olap.metadata.Member value )
    throws javax.olap.OLAPException;

}
