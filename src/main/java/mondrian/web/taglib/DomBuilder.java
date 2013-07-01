/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.web.taglib;

import mondrian.olap.*;

import org.apache.log4j.Logger;

import org.w3c.dom.*;

import java.io.*;
import java.util.List;
import javax.xml.parsers.*;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Transforms a mondrian result into a DOM (Document Object Model).
 *
 * @author Andreas Voss, 22 March, 2002
 */
public class DomBuilder {
    private static final Logger LOGGER = Logger.getLogger(DomBuilder.class);

    Document factory;
    Result result;
    int dimCount;

    protected DomBuilder(Document factory, Result result) {
        this.factory = factory;
        this.result = result;
    }

    public static Document build(Result result)
        throws ParserConfigurationException
    {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setValidating(false);
        dbf.setExpandEntityReferences(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.newDocument();
        Element table = build(doc, result);
        doc.appendChild(table);
        // debug(doc);
        return doc;
    }

    public static Element build(Document factory, Result result) {
        return new DomBuilder(factory, result).build();
    }

    private Element build() {
        dimCount = result.getAxes().length;
        Element mdxtable = factory.createElement("mdxtable");
        Element query = elem("query", mdxtable);
        cdata(Util.unparse(result.getQuery()), query);
        Element head = elem("head", mdxtable);
        Element body = elem("body", mdxtable);
        switch (dimCount) {
        case 0:
            buildRows0Dim(body);
            break;
        case 1:
            buildColumns(head, result.getAxes()[0]);
            buildRows1Dim(body);
            break;
        case 2:
            buildColumns(head, result.getAxes()[0]);
            buildRows2Dim(body, result.getAxes()[1]);
            break;
        default:
            throw new IllegalArgumentException(
                "DomBuilder requires 0, 1 or 2 dimensional result");
        }
        Element slicers = elem("slicers", mdxtable);
        buildSlicer(slicers);
        return mdxtable;
    }

    abstract class AxisBuilder {
        Member[] prevMembers;
        Element[] prevElems;
        int [] prevSpan;

        Element parent;
        List<Position> positions;
        int levels;

        AxisBuilder(Element parent, Axis axis) {
            this.parent   = parent;

            positions = axis.getPositions();
            levels = positions.get(0).size();
            prevMembers = new Member[levels];
            prevElems = new Element[levels];
            prevSpan = new int[levels];
        }

        abstract int getRowCount();
        abstract Element build(int rowIndex);
    }

    class RowBuilder extends AxisBuilder {
        RowBuilder(Element parent, Axis axis) {
            super(parent, axis);
        }

        Element build(int rowIndex) {
            boolean even = (rowIndex % 2 != 0);  // counting starts at row 1
            Element row = elem("row", parent);
            build(row, positions.get(rowIndex), even);
            return row;
        }

        int getRowCount() {
            return positions.size();
        }

        private void build(
            Element row,
            List<Member> currentMembers,
            boolean even)
        {
            for (int i = 0; i < levels; i++) {
                Member currentMember = currentMembers.get(i);
                Member prevMember    = prevMembers[i];
                if (prevMember == null || !prevMember.equals(currentMember)) {
                    Element currentElem =
                        createMemberElem("row-heading", row, currentMember);
                    if (even) {
                        currentElem.setAttribute("style", "even");
                    } else {
                        currentElem.setAttribute("style", "odd");
                    }
                    prevMembers[i] = currentMember;
                    prevElems[i] = currentElem;
                    prevSpan[i] = 1;
                    for (int j = i + 1; j < levels; j++) {
                        prevMembers[j] = null;
                    }
                } else {
                    Element prevElem = prevElems[i];
                    prevElem.setAttribute("style", "span");
                    prevSpan[i] += 1;
                    prevElem.setAttribute(
                        "rowspan", Integer.toString(prevSpan[i]));
                }
            }
        }
    }


    class ColumnBuilder extends AxisBuilder {
        ColumnBuilder(Element parent, Axis axis) {
            super(parent, axis);
        }

        int getRowCount() {
            return levels;
        }

        Element build(int rowIndex) {
            Element row = elem("row", parent);
            if (dimCount > 1 && rowIndex == 0) {
                buildCornerElement(row);
            }
            build(row, rowIndex);
            return row;
        }

        private void build(Element row, int rowIndex) {
            for (int i = 0; i < levels; i++) {
                prevMembers[i] = null;
            }
            for (int i = 0; i < positions.size(); i++) {
                Position position = positions.get(i);
                //Member[] currentMembers = positions.get(i).getMembers();

                for (int j = 0; j < rowIndex - 1; j++) {
                    Member currentMember = position.get(j);
                    if (prevMembers[j] == null
                        || !prevMembers[j].equals(currentMember))
                    {
                        prevMembers[j] = currentMember;
                        for (int k = j + 1; k < levels; k++) {
                            prevMembers[j] = null;
                        }
                    }
                }

                Member currentMember = position.get(rowIndex);
                Member prevMember    = prevMembers[rowIndex];
                if (prevMember == null || !prevMember.equals(currentMember)) {
                    Element currentElem =
                        createMemberElem("column-heading", row, currentMember);
                    prevMembers[rowIndex] = currentMember;
                    prevElems[rowIndex] = currentElem;
                    prevSpan[rowIndex] = 1;
                    for (int j = rowIndex + 1; j < levels; j++) {
                        prevMembers[j] = null;
                    }
                } else {
                    Element prevElem = prevElems[rowIndex];
                    prevElem.setAttribute("style", "span");
                    prevSpan[rowIndex] += 1;
                    prevElem.setAttribute(
                        "colspan", Integer.toString(prevSpan[rowIndex]));
                }
            }
        }

        void buildCornerElement(Element row) {
            Element corner = elem("corner", row);
            corner.setAttribute(
                "rowspan",
                Integer.toString(
                    result.getAxes()[0].getPositions().get(0).size()));
            corner.setAttribute(
                "colspan",
                Integer.toString(
                    result.getAxes()[1].getPositions().get(0).size()));
        }
    }


    private void buildRows2Dim(Element parent, Axis axis) {
        RowBuilder rb = new RowBuilder(parent, axis);
        final int N = rb.getRowCount();
        int[] cellIndex = new int[2];
        for (int i = 0; i < N; i++) {
            Element row = rb.build(i);
            boolean even = (i % 2 != 0);  // counting starts at row 1
            cellIndex[1] = i;
            buildCells(row, cellIndex, even);
        }
    }

    private void buildRows1Dim(Element parent) {
        int[] cellIndex = new int[1];
        Element row = elem("row", parent);
        buildCells(row, cellIndex, false);
    }

    private void buildColumns(Element parent, Axis axis) {
        ColumnBuilder cb = new ColumnBuilder(parent, axis);
        final int N = cb.getRowCount();
        for (int i = 0; i < N; i++) {
            Element row = cb.build(i);
        }
    }


    private void buildCells(Element row, int[] cellIndex, boolean even) {
        int columns = result.getAxes()[0].getPositions().size();
        for (int i = 0; i < columns; i++) {
            cellIndex[0] = i;
            Cell cell = result.getCell(cellIndex);
            buildCell(cell, row, even);
        }
    }

    private void buildCell(Cell cell, Element row, boolean even) {
        Element cellElem = elem("cell", row);
        String s = cell.getFormattedValue();
        if (s == null || s.length() == 0 || s.equals("(null)")) {
            s = "\u00a0"; // &nbsp;
        }
        cellElem.setAttribute("value", s);
        cellElem.setAttribute("style", even ? "even" : "odd");
    }

    private void buildRows0Dim(Element parent) {
        int[] cellIndex = new int[0];
        Element row = elem("row", parent);
        Cell cell = result.getCell(cellIndex);
        buildCell(cell, row, false);
    }

    private void buildSlicer(Element parent) {
        List<Position> positions = result.getSlicerAxis().getPositions();
        for (int i = 0; i < positions.size(); i++) {
            Position position = positions.get(i);
            if (position.size() > 0) {
                Element el = elem("position", parent);
                for (int j = 0; j < position.size(); j++) {
                    createMemberElem("member", el, position.get(j));
                }
            }
        }
    }

    private Element createMemberElem(String name, Element parent, Member m) {
        Element e = elem(name, parent);
        e.setAttribute("caption", m.getCaption());
        e.setAttribute("depth", Integer.toString(m.getLevel().getDepth()));
        //e.setAttribute("name", m.getName());
        //e.setAttribute("qname", m.getQualifiedName());
        e.setAttribute("uname", m.getUniqueName());
        e.setAttribute("colspan", "1");
        e.setAttribute("rowspan", "1");

        // add properties to dom tree
        addMemberProperties(m, e);

        return e;
    }

    private void addMemberProperties(Member m, Element e) {
        for (Property prop : m.getLevel().getProperties()) {
            String propValue = "" + m.getPropertyValue(prop);
            Element propElem = elem("property", e);
            propElem.setAttribute("name", prop.getName());
            propElem.setAttribute("value", propValue);
        }
    }

    private Element elem(String name, Element parent) {
        Element elem = factory.createElement(name);
        parent.appendChild(elem);
        return elem;
    }

    private Object cdata(String content, Element parent) {
        CDATASection section = factory.createCDATASection(content);
        parent.appendChild(section);
        return section;
    }

    private static final String PRETTY_PRINTER =
        ""
        + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n"
        + "<xsl:output method=\"xml\" indent=\"yes\"/>\n"
        + "<xsl:template match=\"*|@*\">\n"
        + "  <xsl:copy>\n"
        + "    <xsl:apply-templates select=\"*|@*\"/>\n"
        + "  </xsl:copy>\n"
        + "</xsl:template>\n"
        + "</xsl:stylesheet>\n";

    public static void debug(Document doc) {
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            StringReader input = new StringReader(PRETTY_PRINTER);
            Templates templates = tf.newTemplates(new StreamSource(input));
            OutputStream result = new ByteArrayOutputStream();
            templates.newTransformer().transform(
                new DOMSource(doc),
                new StreamResult(result));
            LOGGER.debug(result.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

// End DomBuilder.java
