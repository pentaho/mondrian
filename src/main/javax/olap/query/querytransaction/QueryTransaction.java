/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.querytransaction;



public interface QueryTransaction
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getTransactionElement()
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransaction getChild()
    throws javax.olap.OLAPException;

  public void setChild( javax.olap.query.querytransaction.QueryTransaction value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransaction getParent()
    throws javax.olap.OLAPException;

  public void setParent( javax.olap.query.querytransaction.QueryTransaction value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransactionManager getTransactionManager()
    throws javax.olap.OLAPException;

  public void setTransactionManager( javax.olap.query.querytransaction.QueryTransactionManager value )
    throws javax.olap.OLAPException;

}
