/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.analysis.transformation;



public interface TransformationUseClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.analysis.transformation.TransformationUse createTransformationUse( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, java.lang.String _kind, java.lang.String _type )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.analysis.transformation.TransformationUse createTransformationUse();

}
