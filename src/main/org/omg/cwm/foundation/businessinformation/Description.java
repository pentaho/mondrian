package org. omg. cwm. foundation. businessinformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Description extends Namespace {


// class scalar attributes
public String getBody();
public void setBody( String input);
public String getLanguage();
public void setLanguage( String input);
public String getType();
public void setType( String input);


// class references
public void setModelElement( Collection input);
public Collection getModelElement();
public void addModelElement( ModelElement input);
public void removeModelElement( ModelElement input);
// class operations
}


