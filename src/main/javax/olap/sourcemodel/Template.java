/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface Template {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.Source getSource()
    throws javax.olap.OLAPException;

  public void setSource( javax.olap.sourcemodel.Source value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.MetadataState getCurrentState()
    throws javax.olap.OLAPException;

  public void setCurrentState( javax.olap.sourcemodel.MetadataState value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.SourceGenerator getSourceGenerator()
    throws javax.olap.OLAPException;

  public void setSourceGenerator( javax.olap.sourcemodel.SourceGenerator value )
    throws javax.olap.OLAPException;

}
