/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface UnionMember
extends org.omg.java.cwm.objectmodel.core.Attribute {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Expression getMemberCase();

  public void setMemberCase( org.omg.java.cwm.objectmodel.core.Expression value );

  public boolean isDefault();

  public void setDefault( boolean value );

}
