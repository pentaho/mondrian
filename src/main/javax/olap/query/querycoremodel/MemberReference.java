/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface MemberReference
extends javax.olap.query.calculatedmembers.OperatorInput {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Member getMember()
    throws javax.olap.OLAPException;

  public void setMember( javax.olap.metadata.Member value )
    throws javax.olap.OLAPException;

}
