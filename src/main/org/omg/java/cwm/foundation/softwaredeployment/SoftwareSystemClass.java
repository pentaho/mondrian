/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.softwaredeployment;



public interface SoftwareSystemClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem createSoftwareSystem( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, boolean _isAbstract, java.lang.String _type, java.lang.String _subtype, java.lang.String _supplier, java.lang.String _version )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.softwaredeployment.SoftwareSystem createSoftwareSystem();

}
