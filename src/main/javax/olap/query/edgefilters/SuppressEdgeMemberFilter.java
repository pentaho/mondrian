/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.edgefilters;



public interface SuppressEdgeMemberFilter
extends javax.olap.query.querycoremodel.EdgeFilter {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.edgefilters.QualifiedEdgeMemberReference getEdgeMember()
    throws javax.olap.OLAPException;

  public void setEdgeMember( javax.olap.query.edgefilters.QualifiedEdgeMemberReference value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.edgefilters.QualifiedEdgeMemberReference createQualifiedEdgeMemberReference()
    throws javax.olap.OLAPException;

}
