/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querycoremodel;



public interface DimensionStep
extends javax.olap.query.querytransaction.TransactionalObject, javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.querycoremodel.DimensionStepManager getDimensionStepManager()
    throws javax.olap.OLAPException;

  public void setDimensionStepManager( javax.olap.query.querycoremodel.DimensionStepManager value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.CompoundDimensionStep getCompoundDimensionStep()
    throws javax.olap.OLAPException;

  public void setCompoundDimensionStep( javax.olap.query.querycoremodel.CompoundDimensionStep value )
    throws javax.olap.OLAPException;

}
