/*
 * Java(TM) OLAP Interface
 */
package javax.olap.resource;



public interface ConnectionFactory
extends java.io.Serializable, javax.resource.Referenceable {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.resource.Connection getConnection()
    throws javax.olap.OLAPException;

  public javax.olap.resource.Connection getConnection( javax.olap.resource.ConnectionSpec properties )
    throws javax.olap.OLAPException;

  public javax.olap.resource.ConnectionSpec createConnectionSpec()
    throws javax.olap.OLAPException;

  public javax.olap.resource.ResourceAdapterMetaData getMetaData()
    throws javax.olap.OLAPException;

}
