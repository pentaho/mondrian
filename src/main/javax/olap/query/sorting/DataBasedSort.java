/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.sorting;



public interface DataBasedSort
extends javax.olap.query.sorting.DimensionSort {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.QualifiedMemberReference getQualifiedMemberReference()
    throws javax.olap.OLAPException;

  public void setQualifiedMemberReference( javax.olap.query.querycoremodel.QualifiedMemberReference value )
    throws javax.olap.OLAPException;


	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.QualifiedMemberReference createQualifiedMemberReference()
    throws javax.olap.OLAPException;

}
