/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface OperatorReference
extends javax.olap.query.calculatedmembers.OperatorInput {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.OrdinateOperator getOperator()
    throws javax.olap.OLAPException;

  public void setOperator( javax.olap.query.calculatedmembers.OrdinateOperator value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.OrdinateOperator createOrdinateOperator()
    throws javax.olap.OLAPException;

}
