/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface Drill
extends javax.olap.query.dimensionfilters.DimensionFilter {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public javax.olap.query.enumerations.DrillType getDrillType()
    throws javax.olap.OLAPException;

  public void setDrillType( javax.olap.query.enumerations.DrillType value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.metadata.Hierarchy getHierarchy()
    throws javax.olap.OLAPException;

  public void setHierarchy( javax.olap.metadata.Hierarchy value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Level getLevel()
    throws javax.olap.OLAPException;

  public void setLevel( javax.olap.metadata.Level value )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Member getDrillMember()
    throws javax.olap.OLAPException;

  public void setDrillMember( javax.olap.metadata.Member value )
    throws javax.olap.OLAPException;

}
