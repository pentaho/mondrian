/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface LocationClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.businessinformation.Location createLocation( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _locationType, java.lang.String _address, java.lang.String _city, java.lang.String _postCode, java.lang.String _area, java.lang.String _country )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.businessinformation.Location createLocation();

}
