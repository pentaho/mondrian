/*
 * Java(TM) OLAP Interface
 */
package javax.olap.resource;



public interface ConnectionMetaData {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public java.lang.String getEISProductName()
    throws javax.olap.OLAPException;

  public java.lang.String getEISProductVersion()
    throws javax.olap.OLAPException;

  public java.lang.String getUserName()
    throws javax.olap.OLAPException;

}
