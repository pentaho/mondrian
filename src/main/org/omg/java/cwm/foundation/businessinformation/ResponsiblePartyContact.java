/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface ResponsiblePartyContact
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.businessinformation.Contact contact, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getResponsibleParty( org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getContact( org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.businessinformation.Contact contact, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.businessinformation.Contact contact, org.omg.java.cwm.foundation.businessinformation.ResponsibleParty responsibleParty )
    throws javax.jmi.reflect.JmiException;

}
