/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface IntegerInsertOffset
extends javax.olap.query.edgefilters.EdgeInsertOffset, javax.olap.query.dimensionfilters.DimensionInsertOffset {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public int getValue()
    throws javax.olap.OLAPException;

  public void setValue( int value )
    throws javax.olap.OLAPException;

}
