/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface ProviderConnectionClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection createProviderConnection( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isReadOnly )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.ProviderConnection createProviderConnection();

}
