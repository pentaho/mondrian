package javax.jmi.model;



public interface TypedElement extends ModelElement {
    public Classifier getType();
    public void setType(Classifier newValue);
}
