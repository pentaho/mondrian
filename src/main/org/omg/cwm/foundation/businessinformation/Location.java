package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Location extends ModelElement {


// class scalar attributes
public String getLocationType();
public void setLocationType( String input);
public String getAddress();
public void setAddress( String input);
public String getCity();
public void setCity( String input);
public String getPostCode();
public void setPostCode( String input);
public String getArea();
public void setArea( String input);
public String getCountry();
public void setCountry( String input);
// class references
public void setContact( Collection input);
public Collection getContact();
public void addContact( Contact input);
public void removeContact( Contact input);
// class operations
}

