/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface LiteralReference
extends javax.olap.query.calculatedmembers.OperatorInput, javax.olap.query.querycoremodel.SelectedObject, javax.olap.query.derivedattribute.DerivedAttributeComponent {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.Constant getLiteral()
    throws javax.olap.OLAPException;

  public void setLiteral( javax.olap.query.querycoremodel.Constant value )
    throws javax.olap.OLAPException;

}
