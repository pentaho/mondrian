package org. omg. cwm. foundation. keysindexes;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface IndexedFeature extends ModelElement {


// class scalar attributes
public Boolean getIsAscending();
public void setIsAscending( Boolean input);
// class references
public void setFeature( StructuralFeature input);
public StructuralFeature getFeature();
public void setIndex( Index input);
public Index getIndex();
// class operations
}


