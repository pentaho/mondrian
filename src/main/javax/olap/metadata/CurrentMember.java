/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface CurrentMember
extends javax.olap.metadata.Member {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.MemberQuantifierType getMemberQuantifier()
    throws javax.olap.OLAPException;

  public void setMemberQuantifier( javax.olap.metadata.MemberQuantifierType value )
    throws javax.olap.OLAPException;

}
