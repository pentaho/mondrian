/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Timestamp
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.cursor.Timestamp valueOf( java.lang.String s )
    throws javax.olap.OLAPException;

  public java.lang.String toString();

  public int getNanos()
    throws javax.olap.OLAPException;

  public void setNanos( int n )
    throws javax.olap.OLAPException;

  public boolean equals( javax.olap.cursor.Timestamp ts );

  public boolean equals( java.lang.Object ts );

  public boolean before( javax.olap.cursor.Timestamp ts )
    throws javax.olap.OLAPException;

  public boolean after( javax.olap.cursor.Timestamp ts )
    throws javax.olap.OLAPException;

}
