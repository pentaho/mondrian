/*
 * Java(TM) OLAP Interface
 */
package javax.olap.resource;



public interface ResourceAdapterMetaData {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public java.lang.String getAdapterName()
    throws javax.olap.OLAPException;

  public java.lang.String getAdapterShortDescription()
    throws javax.olap.OLAPException;

  public java.lang.String getAdapterVendorName()
    throws javax.olap.OLAPException;

  public java.lang.String getAdapterVersion()
    throws javax.olap.OLAPException;

  public java.lang.String getSpecificationTitle()
    throws javax.olap.OLAPException;

  public java.lang.String getSpecificationVersion()
    throws javax.olap.OLAPException;

  public java.lang.String getSpecificationVendor()
    throws javax.olap.OLAPException;

  public java.lang.String getComplianceLevel()
    throws javax.olap.OLAPException;

  public java.lang.String getSpecVersion()
    throws javax.olap.OLAPException;

}
