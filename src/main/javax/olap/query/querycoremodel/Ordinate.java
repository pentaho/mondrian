/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface Ordinate
extends javax.olap.query.querycoremodel.QueryObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getCalculatedMember()
    throws javax.olap.OLAPException;

  public java.util.Collection getOperatorInput()
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.CalculatedMember createCalculatedMember()
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.CalculatedMember createCalculatedMemberBefore( javax.olap.query.calculatedmembers.CalculatedMember member )
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.CalculatedMember createCalculatedMemberAfter( javax.olap.query.calculatedmembers.CalculatedMember member )
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.OperatorInput createOperatorInput( javax.olap.query.enumerations.OperatorInputType type )
    throws javax.olap.OLAPException;

}
