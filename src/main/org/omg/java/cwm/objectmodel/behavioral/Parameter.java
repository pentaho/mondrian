/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface Parameter
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Expression getDefaultValue();

  public void setDefaultValue( org.omg.java.cwm.objectmodel.core.Expression value );

  public org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKind getKind();

  public void setKind( org.omg.java.cwm.objectmodel.behavioral.ParameterDirectionKind value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature getBehavioralFeature();

  public void setBehavioralFeature( org.omg.java.cwm.objectmodel.behavioral.BehavioralFeature value );

  public org.omg.java.cwm.objectmodel.behavioral.Event getEvent();

  public void setEvent( org.omg.java.cwm.objectmodel.behavioral.Event value );

  public org.omg.java.cwm.objectmodel.core.Classifier getType();

  public void setType( org.omg.java.cwm.objectmodel.core.Classifier value );

}
