/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface DataProviderClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.DataProvider createDataProvider( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _pathname, boolean _isCaseSensitive )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.DataProvider createDataProvider();

}
