package javax.jmi.model;



public interface Tag extends ModelElement {
    public String getTagId();
    public void setTagId(String newValue);
    public java.util.List getValues();
    public java.util.Collection getElements();
}
