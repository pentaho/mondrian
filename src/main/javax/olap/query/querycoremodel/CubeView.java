/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface CubeView
extends javax.olap.query.querycoremodel.QueryObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getOrdinateEdge()
    throws javax.olap.OLAPException;

  public java.util.Collection getPageEdge()
    throws javax.olap.OLAPException;

  public java.util.List getDefaultOrdinatePrecedence()
    throws javax.olap.OLAPException;

  public java.util.Collection getCubeCursor()
    throws javax.olap.OLAPException;

  public java.util.Collection getCalculationRelationship()
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.cursor.CubeCursor createCursor()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView createPageEdge()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView createOrdinateEdge()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView createOrdinateEdgeBefore( javax.olap.query.querycoremodel.EdgeView member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView createOrdinateEdgeAfter( javax.olap.query.querycoremodel.EdgeView member )
    throws javax.olap.OLAPException;

  public javax.olap.query.calculatedmembers.CalculationRelationship createCalculationRelationship()
    throws javax.olap.OLAPException;

  public void pivot( javax.olap.query.querycoremodel.DimensionView dv, javax.olap.query.querycoremodel.EdgeView source, javax.olap.query.querycoremodel.EdgeView target )
    throws javax.olap.OLAPException;

  public void pivot( javax.olap.query.querycoremodel.DimensionView dv, javax.olap.query.querycoremodel.EdgeView source, javax.olap.query.querycoremodel.EdgeView target, int position )
    throws javax.olap.OLAPException;

  public void rotate( javax.olap.query.querycoremodel.EdgeView edv1, javax.olap.query.querycoremodel.EdgeView edv2 )
    throws javax.olap.OLAPException;

}
