/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.core;



public interface ImportedElements
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.objectmodel.core.ModelElement importedElement, org.omg.java.cwm.objectmodel.core.Package importer )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getImporter( org.omg.java.cwm.objectmodel.core.ModelElement importedElement )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getImportedElement( org.omg.java.cwm.objectmodel.core.Package importer )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.objectmodel.core.ModelElement importedElement, org.omg.java.cwm.objectmodel.core.Package importer )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.objectmodel.core.ModelElement importedElement, org.omg.java.cwm.objectmodel.core.Package importer )
    throws javax.jmi.reflect.JmiException;

}
