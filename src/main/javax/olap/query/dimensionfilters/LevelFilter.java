/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface LevelFilter
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Level getLevel()
    throws javax.olap.OLAPException;

  public void setLevel( javax.olap.metadata.Level value )
    throws javax.olap.OLAPException;

}
