/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.objectmodel.relationships;



public interface RelationshipsPackage
extends javax.jmi.reflect.RefPackage {

  public org.omg.java.cwm.objectmodel.core.CorePackage getCore();

  public org.omg.java.cwm.objectmodel.relationships.AssociationClass getAssociation();

  public org.omg.java.cwm.objectmodel.relationships.AssociationEndClass getAssociationEnd();

  public org.omg.java.cwm.objectmodel.relationships.GeneralizationClass getGeneralization();

  public org.omg.java.cwm.objectmodel.relationships.ParentElement getParentElement();

  public org.omg.java.cwm.objectmodel.relationships.ChildElement getChildElement();

}
