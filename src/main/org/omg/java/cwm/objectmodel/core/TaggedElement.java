/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface TaggedElement
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.objectmodel.core.TaggedValue taggedValue )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getTaggedValue( org.omg.java.cwm.objectmodel.core.ModelElement modelElement )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.ModelElement getModelElement( org.omg.java.cwm.objectmodel.core.TaggedValue taggedValue )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.objectmodel.core.TaggedValue taggedValue )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement modelElement, org.omg.java.cwm.objectmodel.core.TaggedValue taggedValue )
    throws javax.jmi.reflect.JmiException;

}
