/*
 * Java(TM) OLAP Interface
 */
package javax.olap.resource;



public interface Abortable {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public void abort()
    throws javax.olap.OLAPException;

}
