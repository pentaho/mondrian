/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface Location
extends org.omg.java.cwm.objectmodel.core.ModelElement {

	// ------------------------------------------------
	// -----   Attribute-Generated                -----
	// ------------------------------------------------

  public java.lang.String getLocationType();

  public void setLocationType( java.lang.String value );

  public java.lang.String getAddress();

  public void setAddress( java.lang.String value );

  public java.lang.String getCity();

  public void setCity( java.lang.String value );

  public java.lang.String getPostCode();

  public void setPostCode( java.lang.String value );

  public java.lang.String getArea();

  public void setArea( java.lang.String value );

  public java.lang.String getCountry();

  public void setCountry( java.lang.String value );

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getContact();

}
