package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface ResourceLocator extends ModelElement {


// class scalar attributes
public String getUrl();
public void setUrl( String input);
// class references
public void setContact( Collection input);
public Collection getContact();
public void addContact( Contact input);
public void removeContact( Contact input);
// class operations
}


