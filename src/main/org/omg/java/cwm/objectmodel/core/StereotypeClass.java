/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface StereotypeClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.core.Stereotype createStereotype( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _baseClass )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Stereotype createStereotype();

}
