/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface EdgeView
extends javax.olap.query.querycoremodel.Ordinate {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.CubeView getPageOwner()
    throws javax.olap.OLAPException;

  public void setPageOwner( javax.olap.query.querycoremodel.CubeView value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.CubeView getOrdinateOwner()
    throws javax.olap.OLAPException;

  public void setOrdinateOwner( javax.olap.query.querycoremodel.CubeView value )
    throws javax.olap.OLAPException;

  public java.util.List getDimensionView()
    throws javax.olap.OLAPException;

  public java.util.Collection getEdgeCursor()
    throws javax.olap.OLAPException;

  public java.util.List getSegment()
    throws javax.olap.OLAPException;

  public java.util.List getEdgeFilter()
    throws javax.olap.OLAPException;

  public java.util.Collection getTuple()
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.cursor.EdgeCursor createCursor()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Segment createSegment()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Segment createSegmentBefore( javax.olap.query.querycoremodel.Segment member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Segment createSegmentAfter( javax.olap.query.querycoremodel.Segment member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeFilter createEdgeFilter( javax.olap.query.enumerations.EdgeFilterType type )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeFilter createEdgeFilterBefore( javax.olap.query.enumerations.EdgeFilterType type, javax.olap.query.querycoremodel.EdgeFilter member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeFilter createEdgeFilterAfter( javax.olap.query.enumerations.EdgeFilterType type, javax.olap.query.querycoremodel.EdgeFilter member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Tuple createTuple()
    throws javax.olap.OLAPException;

  public javax.olap.query.edgefilters.CurrentEdgeMember createCurrentEdgeMember()
    throws javax.olap.OLAPException;

}
