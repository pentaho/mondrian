package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface ResponsibleParty extends Namespace {


// class scalar attributes
public String getResponsibility();
public void setResponsibility( String input);
// class references
public void setContact( Collection input);
public List getContact();
public void addContact( Contact input);
public void removeContact( Contact input);
public void addContactBefore( Contact before, Contact input);
public void addContactAfter( Contact before, Contact input);
public void moveContactBefore( Contact before, Contact input);
public void moveContactAfter( Contact before, Contact input);
public void setModelElement( Collection input);
public Collection getModelElement();
public void addModelElement( ModelElement input);
public void removeModelElement( ModelElement input);
// class operations
}

