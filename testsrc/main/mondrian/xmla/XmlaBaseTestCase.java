package mondrian.xmla;

import mondrian.test.FoodMartTestCase;
import mondrian.tui.XmlaSupport;
import mondrian.olap.Util;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import java.io.IOException;
import java.io.File;
import java.util.Properties;

/**
 * Extends FoodMartTestCase, adding support for testing XMLA specific
 * functionality, for example LAST_SCHEMA_UPDATE
 *
 * @author mkambol
 * @version $Id$
 */

public abstract class XmlaBaseTestCase extends FoodMartTestCase {
    protected static final String LAST_SCHEMA_UPDATE_DATE_PROP = "last.schema.update.date";
    protected static final String LAST_SCHEMA_UPDATE_DATE = "somedate";
    private static final String LAST_SCHEMA_UPDATE_NODE_NAME = "LAST_SCHEMA_UPDATE";

    public XmlaBaseTestCase() {
    }

    public XmlaBaseTestCase(String name) {
        super(name);
    }

    protected Document replaceLastSchemaUpdateDate(Document doc) {
        NodeList elements = doc.getElementsByTagName(LAST_SCHEMA_UPDATE_NODE_NAME);
        if(elements.getLength() ==0){
            return doc;
        }

        Node lastSchemaUpdateNode = elements.item(0);
        lastSchemaUpdateNode.setTextContent(LAST_SCHEMA_UPDATE_DATE);
        return doc;
    }

}
