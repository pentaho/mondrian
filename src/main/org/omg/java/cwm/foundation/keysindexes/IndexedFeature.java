/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface IndexedFeature
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.Boolean isAscending();

  public void setAscending( java.lang.Boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.StructuralFeature getFeature();

  public void setFeature( org.omg.java.cwm.objectmodel.core.StructuralFeature value );

  public org.omg.java.cwm.foundation.keysindexes.Index getIndex();

  public void setIndex( org.omg.java.cwm.foundation.keysindexes.Index value );

}
