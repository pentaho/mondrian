/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.behavioral;



public interface Argument
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.core.Expression getValue();

  public void setValue( org.omg.java.cwm.objectmodel.core.Expression value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public org.omg.java.cwm.objectmodel.behavioral.CallAction getCallAction();

  public void setCallAction( org.omg.java.cwm.objectmodel.behavioral.CallAction value );

}
