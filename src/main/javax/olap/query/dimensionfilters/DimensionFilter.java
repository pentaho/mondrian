/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface DimensionFilter
extends javax.olap.query.querycoremodel.DimensionStep {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.SetActionType getSetAction()
    throws javax.olap.OLAPException;

  public void setSetAction( javax.olap.query.enumerations.SetActionType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.dimensionfilters.DimensionInsertOffset getDimensionInsertOffset()
    throws javax.olap.OLAPException;

  public void setDimensionInsertOffset( javax.olap.query.dimensionfilters.DimensionInsertOffset value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.dimensionfilters.DimensionInsertOffset createDimensionInsertOffset( javax.olap.query.enumerations.DimensionInsertOffsetType type )
    throws javax.olap.OLAPException;

}
