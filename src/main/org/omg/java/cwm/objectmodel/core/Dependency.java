/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface Dependency
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.String getKind();

  public void setKind( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getClient();

  public java.util.Collection getSupplier();

}
