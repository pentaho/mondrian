/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface ClassifierAlias
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.Classifier type, org.omg.java.cwm.foundation.datatypes.TypeAlias alias )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getAlias( org.omg.java.cwm.objectmodel.core.Classifier type )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.objectmodel.core.Classifier getType( org.omg.java.cwm.foundation.datatypes.TypeAlias alias )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.Classifier type, org.omg.java.cwm.foundation.datatypes.TypeAlias alias )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.Classifier type, org.omg.java.cwm.foundation.datatypes.TypeAlias alias )
    throws javax.jmi.reflect.JmiException;

}
