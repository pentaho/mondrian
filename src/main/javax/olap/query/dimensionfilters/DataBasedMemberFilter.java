/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface DataBasedMemberFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isBasedOnPercent()
    throws javax.olap.OLAPException;

  public void setBasedOnPercent( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.dimensionfilters.DataBasedMemberFilterInput getInput()
    throws javax.olap.OLAPException;

  public void setInput( javax.olap.query.dimensionfilters.DataBasedMemberFilterInput value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.dimensionfilters.DataBasedMemberFilterInput createDataBasedMemberFilterInput( javax.olap.query.enumerations.DataBasedMemberFilterInputType type )
    throws javax.olap.OLAPException;

}
