/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.sorting;



public interface HierarchicalSort
extends javax.olap.query.sorting.DimensionSort {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.HierarchicalSortType getHierarchicalSortType()
    throws javax.olap.OLAPException;

  public void setHierarchicalSortType( javax.olap.query.enumerations.HierarchicalSortType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Hierarchy getBasedOn()
    throws javax.olap.OLAPException;

  public void setBasedOn( javax.olap.metadata.Hierarchy value )
    throws javax.olap.OLAPException;

}
