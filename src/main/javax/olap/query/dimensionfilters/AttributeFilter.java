/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface AttributeFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

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

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Attribute getAttribute()
    throws javax.olap.OLAPException;

  public void setAttribute( org.omg.java.cwm.objectmodel.core.Attribute value )
    throws javax.olap.OLAPException;

}
