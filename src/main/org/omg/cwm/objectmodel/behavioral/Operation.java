package org. omg. cwm. objectmodel. behavioral;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface Operation extends BehavioralFeature {


// class scalar attributes
public Boolean getIsAbstract();
public void setIsAbstract( Boolean input);
// class references
public void setMethod( Collection input);
public Collection getMethod();
public void addMethod( Method input);
public void removeMethod( Method input);
// class operations
}
