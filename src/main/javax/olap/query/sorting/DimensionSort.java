/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.sorting;



public interface DimensionSort
extends javax.olap.query.querycoremodel.DimensionStep {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.SortType getDirection()
    throws javax.olap.OLAPException;

  public void setDirection( javax.olap.query.enumerations.SortType value )
    throws javax.olap.OLAPException;

}
