/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.edgefilters;



public interface CurrentEdgeMember
extends javax.olap.query.querycoremodel.Tuple {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.MemberQuantifierType getMemberQuantifer()
    throws javax.olap.OLAPException;

  public void setMemberQuantifer( javax.olap.metadata.MemberQuantifierType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getMemberReference()
    throws javax.olap.OLAPException;

}
