/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.calculatedmembers;



public interface CalculationRelationship
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.CalculatedMember getSuperiorPrecedence()
    throws javax.olap.OLAPException;

  public void setSuperiorPrecedence( javax.olap.query.calculatedmembers.CalculatedMember value )
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.CalculatedMember getInferiorPrecedence()
    throws javax.olap.OLAPException;

  public void setInferiorPrecedence( javax.olap.query.calculatedmembers.CalculatedMember value )
    throws javax.olap.OLAPException;

}
