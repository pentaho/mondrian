package javax.jmi.reflect;

import java.util.List;

public interface RefFeatured extends RefBaseObject {
    public void refSetValue(RefObject feature, Object value);
    public void refSetValue(String featureName, Object value);
    public Object refGetValue(RefObject feature);
    public Object refGetValue(String featureName);
    public Object refInvokeOperation(RefObject requestedOperation, List args) throws RefException;
    public Object refInvokeOperation(String requestedOperation, List args) throws RefException;
}
