/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface ExceptionMemberFilter
extends javax.olap.query.dimensionfilters.DataBasedMemberFilter {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.OperatorType getOp()
    throws javax.olap.OLAPException;

  public void setOp( javax.olap.query.enumerations.OperatorType value )
    throws javax.olap.OLAPException;

  public java.lang.Object getRhs()
    throws javax.olap.OLAPException;

  public void setRhs( java.lang.Object value )
    throws javax.olap.OLAPException;

}
