/*
 * Java(TM) OLAP Interface
 */
package javax.olap.resource;



public interface Connection
extends javax.olap.resource.Abortable {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public void close()
    throws javax.olap.OLAPException;

  public javax.olap.resource.ConnectionMetaData getMetaData()
    throws javax.olap.OLAPException;

  public javax.jmi.reflect.RefPackage getTopLevelPackage()
    throws javax.olap.OLAPException;

  public java.util.Collection getSchemas()
    throws javax.olap.OLAPException;

  public javax.olap.metadata.Schema getCurrentSchema()
    throws javax.olap.OLAPException;

  public void setCurrentSchema( javax.olap.metadata.Schema schema )
    throws javax.olap.OLAPException;

  public java.util.Collection getDimensions()
    throws javax.olap.OLAPException;

  public java.util.Collection getCubes()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.CubeView createCubeView()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.DimensionView createDimensionView( javax.olap.metadata.Dimension dimension )
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.EdgeView createEdgeView()
    throws javax.olap.OLAPException;

  public javax.olap.query.querycoremodel.Constant createConstant()
    throws javax.olap.OLAPException;

  public javax.olap.metadata.MemberObjectFactories getMemberObjectFactories()
    throws javax.olap.OLAPException;

  public javax.olap.query.querytransaction.QueryTransactionManager getQueryTransactionManager()
    throws javax.olap.OLAPException;

}