/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface HierarchyFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.HierarchyFilterType getHierarchyFilterType()
    throws javax.olap.OLAPException;

  public void setHierarchyFilterType( javax.olap.query.enumerations.HierarchyFilterType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Hierarchy getHierarchy()
    throws javax.olap.OLAPException;

  public void setHierarchy( javax.olap.metadata.Hierarchy value )
    throws javax.olap.OLAPException;

}
