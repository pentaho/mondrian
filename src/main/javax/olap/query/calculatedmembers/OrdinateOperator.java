/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.calculatedmembers;



public interface OrdinateOperator
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.Operator getOperator()
    throws javax.olap.OLAPException;

  public void setOperator( javax.olap.query.enumerations.Operator value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getInput()
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.NullHandling getNullHandling()
    throws javax.olap.OLAPException;

  public void setNullHandling( javax.olap.query.calculatedmembers.NullHandling value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.NullHandling createNullHandling()
    throws javax.olap.OLAPException;

}
