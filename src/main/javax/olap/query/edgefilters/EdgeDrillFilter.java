/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.edgefilters;



public interface EdgeDrillFilter
extends javax.olap.query.querycoremodel.EdgeFilter {

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

  public javax.olap.metadata.Member getDrillMember()
    throws javax.olap.OLAPException;

  public void setDrillMember( javax.olap.metadata.Member value )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Tuple getTuple()
    throws javax.olap.OLAPException;

  public void setTuple( javax.olap.query.querycoremodel.Tuple value )
    throws javax.olap.OLAPException;

}
