package org. omg. cwm. foundation. softwaredeployment;
import java. util. List; import java. util. Collection;
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*;
import org. omg. cwm. foundation. businessinformation.*;
public interface SoftwareSystem extends Subsystem {


// class scalar attributes
public String getType();
public void setType( String input);
public String getSubtype();
public void setSubtype( String input);
public String getSupplier();
public void setSupplier( String input);
public String getVersion();
public void setVersion( String input);


// class references
public void setTypespace( Collection input);
public Collection getTypespace();
public void addTypespace( TypeSystem input);
public void removeTypespace( TypeSystem input);
// class operations
}


