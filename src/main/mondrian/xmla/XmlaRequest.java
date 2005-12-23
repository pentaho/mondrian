package mondrian.xmla;

import java.util.Map;

/**
 * XML/A request interface.
 *
 * @author: Gang Chen
 */
public interface XmlaRequest {

    /**
     * Indicate DISCOVER or EXECUTE method. 
     */
    int getMethod();

    /**
     * Properties of XML/A request.
     */
    Map getProperties();

    /**
     * Restrictions of DISCOVER method. 
     */
    Map getRestrictions();

    /**
     * Statement of EXECUTE method.
     */
    String getStatement();

    /** 
     * Role binds with this XML/A reqeust.
     */
    String getRole();

    /**
     * Request type of DISCOVER method.
     */
    String getRequestType();

    /**
     * Indicate whether statement is a drill through statement of 
     * EXECUTE method.
     */
    boolean isDrillThrough();
    
    /**
     * Drill through option: max returning rows of query.
     * 
     * Value -1 means this option isn't provided.
     */
    int drillThroughMaxRows();
    
    /**
     * Drill through option: first returning row of query.
     * 
     * Value -1 means this option isn't provided.
     */
    int drillThroughFirstRowset();

}
