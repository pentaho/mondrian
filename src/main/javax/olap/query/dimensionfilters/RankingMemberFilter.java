/*
 * Java(TM) OLAP Interface
 */
package javax.olap.query.dimensionfilters;



public interface RankingMemberFilter
extends javax.olap.query.dimensionfilters.DataBasedMemberFilter {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public int getTop()
    throws javax.olap.OLAPException;

  public void setTop( int value )
    throws javax.olap.OLAPException;

  public boolean isTopPercent()
    throws javax.olap.OLAPException;

  public void setTopPercent( boolean value )
    throws javax.olap.OLAPException;

  public int getBottom()
    throws javax.olap.OLAPException;

  public void setBottom( int value )
    throws javax.olap.OLAPException;

  public boolean isBottomPercent()
    throws javax.olap.OLAPException;

  public void setBottomPercent( boolean value )
    throws javax.olap.OLAPException;

  public javax.olap.query.enumerations.RankingType getType()
    throws javax.olap.OLAPException;

  public void setType( javax.olap.query.enumerations.RankingType value )
    throws javax.olap.OLAPException;

}
