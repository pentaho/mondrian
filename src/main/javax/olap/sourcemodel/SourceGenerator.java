/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface SourceGenerator {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.Source generateSource( javax.olap.sourcemodel.MetadataState state )
    throws javax.olap.OLAPException;

}
