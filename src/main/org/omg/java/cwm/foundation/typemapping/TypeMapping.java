/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.typemapping;



public interface TypeMapping
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isBestMatch();

  public void setBestMatch( boolean value );

  public boolean isLossy();

  public void setLossy( boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Classifier getSourceType();

  public void setSourceType( org.omg.java.cwm.objectmodel.core.Classifier value );

  public org.omg.java.cwm.objectmodel.core.Classifier getTargetType();

  public void setTargetType( org.omg.java.cwm.objectmodel.core.Classifier value );

}
