/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface ComponentClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.Component createComponent( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.Component createComponent();

}
