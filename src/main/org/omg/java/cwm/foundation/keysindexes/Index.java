/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.keysindexes;



public interface Index
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public boolean isPartitioning();

  public void setPartitioning( boolean value );

  public boolean isSorted();

  public void setSorted( boolean value );

  public boolean isUnique();

  public void setUnique( boolean value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.List getIndexedFeature();

  public org.omg.java.cwm.objectmodel.core.CoreClass getSpannedClass();

  public void setSpannedClass( org.omg.java.cwm.objectmodel.core.CoreClass value );

}
