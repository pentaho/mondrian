/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.edgefilters;



public interface TupleFilter
extends javax.olap.query.querycoremodel.EdgeFilter {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.Tuple getTuple()
    throws javax.olap.OLAPException;

  public void setTuple( javax.olap.query.querycoremodel.Tuple value )
    throws javax.olap.OLAPException;

  public javax.olap.query.edgefilters.EdgeInsertOffset getEdgeInsertOffset()
    throws javax.olap.OLAPException;

  public void setEdgeInsertOffset( javax.olap.query.edgefilters.EdgeInsertOffset value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.edgefilters.EdgeInsertOffset createEdgeInsertOffset( javax.olap.query.enumerations.EdgeInsertOffsetType type )
    throws javax.olap.OLAPException;

}
