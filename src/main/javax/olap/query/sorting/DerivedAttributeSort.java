/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.sorting;



public interface DerivedAttributeSort
extends javax.olap.query.sorting.DimensionSort {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.derivedattribute.DerivedAttribute getBasedOn()
    throws javax.olap.OLAPException;

  public void setBasedOn( javax.olap.query.derivedattribute.DerivedAttribute value )
    throws javax.olap.OLAPException;

}
