/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface Feature
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ScopeKind getOwnerScope();

  public void setOwnerScope( org.omg.java.cwm.objectmodel.core.ScopeKind value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Classifier getOwner();

  public void setOwner( org.omg.java.cwm.objectmodel.core.Classifier value );

}
