/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.calculatedmembers;



public interface CalculatedMember
extends javax.olap.metadata.Member {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isHidden()
    throws javax.olap.OLAPException;

  public void setHidden( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getMasterPrecedence()
    throws javax.olap.OLAPException;

  public java.util.Collection getSlavePrecedence()
    throws javax.olap.OLAPException;

  public java.util.Collection getAttributeValue()
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.OrdinateOperator getOperator()
    throws javax.olap.OLAPException;

  public void setOperator( javax.olap.query.calculatedmembers.OrdinateOperator value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.calculatedmembers.AttributeValue createAttributeValue()
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.OrdinateOperator createOrdinateOperator()
    throws javax.olap.OLAPException;

}
