/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface MemberListFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.MemberList getMemberList()
    throws javax.olap.OLAPException;

  public void setMemberList( javax.olap.metadata.MemberList value )
    throws javax.olap.OLAPException;

}
