/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface TaggedValue
extends org.omg.java.cwm.objectmodel.core.Element {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.String getTag();

  public void setTag( java.lang.String value );

  public java.lang.String getValue();

  public void setValue( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.ModelElement getModelElement();

  public void setModelElement( org.omg.java.cwm.objectmodel.core.ModelElement value );

  public org.omg.java.cwm.objectmodel.core.Stereotype getStereotype();

  public void setStereotype( org.omg.java.cwm.objectmodel.core.Stereotype value );

}
