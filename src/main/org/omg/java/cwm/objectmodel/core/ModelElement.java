/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ModelElement
extends org.omg.java.cwm.objectmodel.core.Element {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.String getName();

  public void setName( java.lang.String value );

  public org.omg.java.cwm.objectmodel.core.VisibilityKind getVisibility();

  public void setVisibility( org.omg.java.cwm.objectmodel.core.VisibilityKind value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getClientDependency();

  public java.util.Collection getConstraint();

  public java.util.Collection getImporter();

  public org.omg.java.cwm.objectmodel.core.Namespace getNamespace();

  public void setNamespace( org.omg.java.cwm.objectmodel.core.Namespace value );

}
