/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.relationships;



public interface GeneralizationClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.objectmodel.relationships.Generalization createGeneralization( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.relationships.Generalization createGeneralization();

}
