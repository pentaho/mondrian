package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Email extends ModelElement
{
// class scalar attributes
public String getEmailAddress();


public void setEmailAddress( String input);
public String getEmailType();
public void setEmailType( String input);


// class references
public void setContact( Collection input);
public Collection getContact();
public void addContact( Contact input);
public void removeContact( Contact input);
// class operations
}

