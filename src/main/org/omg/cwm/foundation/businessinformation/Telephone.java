package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Telephone extends ModelElement {


// class scalar attributes
public String getPhoneNumber();
public void setPhoneNumber( String input);
public String getPhoneType();
public void setPhoneType( String input);


// class references
public void setContact( Collection input);
public Collection getContact();
public void addContact( Contact input);
public void removeContact( Contact input);
// class operations
}

