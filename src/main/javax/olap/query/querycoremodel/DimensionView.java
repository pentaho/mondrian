/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface DimensionView
extends javax.olap.query.querycoremodel.Ordinate {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isDistinct()
    throws javax.olap.OLAPException;

  public void setDistinct( boolean value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.EdgeView getEdgeView()
    throws javax.olap.OLAPException;

  public void setEdgeView( javax.olap.query.querycoremodel.EdgeView value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Dimension getDimension()
    throws javax.olap.OLAPException;

  public void setDimension( javax.olap.metadata.Dimension value )
    throws javax.olap.OLAPException;

  public java.util.Collection getDimensionStepManager()
    throws javax.olap.OLAPException;

  public java.util.Collection getDimensionCursor()
    throws javax.olap.OLAPException;

  public java.util.List getSelectedObject()
    throws javax.olap.OLAPException;

  public java.util.Collection getDerivedAttribute()
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.cursor.DimensionCursor createCursor()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.SelectedObject createSelectedObject( javax.olap.query.enumerations.SelectedObjectType type )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.SelectedObject createSelectedObjectBefore( javax.olap.query.enumerations.SelectedObjectType type, javax.olap.query.querycoremodel.SelectedObject member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.SelectedObject createSelectedObjectAfter( javax.olap.query.enumerations.SelectedObjectType type, javax.olap.query.querycoremodel.SelectedObject member )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.DimensionStepManager createDimensionStepManager()
    throws javax.olap.OLAPException;

  public javax.olap.query.derivedattribute.DerivedAttribute createDerivedAttribute()
    throws javax.olap.OLAPException;

}
