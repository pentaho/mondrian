/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querytransaction;



public interface QueryTransactionManager
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getQueryTransaction()
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransaction getCurrentTransaction()
    throws javax.olap.OLAPException;

  public void setCurrentTransaction( javax.olap.query.querytransaction.QueryTransaction value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.query.querytransaction.QueryTransaction beginRootTransaction()
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransaction beginChildSubTransaction()
    throws javax.olap.OLAPException;

  public void prepareCurrentTransaction()
    throws javax.olap.OLAPException;

  public void commitCurrentTransaction()
    throws javax.olap.OLAPException;

  public void rollbackCurrentTransaction()
    throws javax.olap.OLAPException;

}
