/*
 * Java(TM) OLAP Interface
 */
package javax.olap.cursor;



public interface Cursor
extends javax.olap.query.querycoremodel.NamedObject {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public java.lang.Object clone();

  public boolean equals( java.lang.Object arg0 );

  public int hashCode();

}
