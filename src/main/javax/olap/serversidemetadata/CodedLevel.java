/*
 * Java(TM) OLAP Interface
 */
package javax.olap.serversidemetadata;



public interface CodedLevel
extends javax.olap.metadata.Level {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.foundation.expressions.ExpressionNode getEncoding()
    throws javax.olap.OLAPException;

  public void setEncoding( org.omg.java.cwm.foundation.expressions.ExpressionNode value )
    throws javax.olap.OLAPException;

}
