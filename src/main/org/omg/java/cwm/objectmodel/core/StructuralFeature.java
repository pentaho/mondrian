/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface StructuralFeature
extends org.omg.java.cwm.objectmodel.core.Feature {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ChangeableKind getChangeability();

  public void setChangeability( org.omg.java.cwm.objectmodel.core.ChangeableKind value );

  public org.omg.java.cwm.objectmodel.core.Multiplicity getMultiplicity();

  public void setMultiplicity( org.omg.java.cwm.objectmodel.core.Multiplicity value );

  public org.omg.java.cwm.objectmodel.core.OrderingKind getOrdering();

  public void setOrdering( org.omg.java.cwm.objectmodel.core.OrderingKind value );

  public org.omg.java.cwm.objectmodel.core.ScopeKind getTargetScope();

  public void setTargetScope( org.omg.java.cwm.objectmodel.core.ScopeKind value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Classifier getType();

  public void setType( org.omg.java.cwm.objectmodel.core.Classifier value );

}
