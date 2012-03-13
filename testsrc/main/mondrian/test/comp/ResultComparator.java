/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test.comp;

import mondrian.olap.*;
import mondrian.test.TestContext;

import junit.framework.Assert;

import org.w3c.dom.*;

import java.util.HashSet;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;

/**
 * Compares the {@link Result} produced by a query with the expected result
 * read from an XML file.
 *
 * @see ResultComparatorTest
 */
class ResultComparator {
    private final Element xmlRoot;
    private final Result result;

    public ResultComparator(Element xmlRoot, Result result) {
        this.xmlRoot = xmlRoot;
        this.result = result;
    }

    public void compareResults() {
        compareSlicers();
        compareColumns();
        compareRows();
        compareData();
    }

    private void compareSlicers() {
        NodeList slicerList = xmlRoot.getElementsByTagName("slicer");

        Cube cube = result.getQuery().getCube();
        HashSet<String> defaultDimMembers = new HashSet<String>();

        for (Dimension dim : cube.getDimensionList()) {
            for (Hierarchy hierarchy : dim.getHierarchyList()) {
                String uniqueName =
                    hierarchy.getDefaultMember().getUniqueName();
                defaultDimMembers.add(uniqueName);
            }
        }

        Axis slicerAxis = result.getSlicerAxis();
        List<Member> members = slicerAxis.getPositions().get(0);

        Element slicerTuple = (Element) slicerList.item(0);
        if (slicerTuple == null) {
            _assertEquals("Expected no slicers", 0, members.size());
            return;
        }

        final Element tuples =
            (Element) slicerTuple.getElementsByTagName("tuples").item(0);
        final Element tuple =
            (Element) tuples.getElementsByTagName("tuple").item(0);
        NodeList expectedTuple = tuple.getElementsByTagName("member");

        // For each of the expected members, make sure that it's either in the
        // result members[] array or the default member for the dimension.
        int numMembers = expectedTuple.getLength();
        int seenMembers = 0;

        for (int idx = 0; idx < numMembers; idx++) {
            String expectedMemberName =
                expectedTuple.item(idx).getFirstChild().getNodeValue();
            expectedMemberName = foo(expectedMemberName);
            if (resultMembersContainsExpected(expectedMemberName, members)) {
                seenMembers++;
            } else if (defaultDimMembers.contains(expectedMemberName)) {
            } else {
                Assert.fail("Missing slicer: " + expectedMemberName);
            }
        }

        _assertEquals(
            "The query returned more slicer members than were expected",
            members.size(),
            seenMembers);
    }

    /**
     * @see Util#deprecated(Object) TODO: upgrade ref logs and remove this
     * hackery
     */
    private String foo(String expectedMemberName) {
        return expectedMemberName
            .replace("[Product].", "[Product].[Products].")
            .replace("[Customers].", "[Customer].[Customers].")
            .replace("[Marital Status].", "[Customer].[Marital Status].")
            .replace("[Gender].", "[Customer].[Gender].")
            .replace("[Education Level].", "[Customer].[Education Level].")
            .replace("[Yearly Income].", "[Customer].[Yearly Income].")
            .replace("[Time].", "[Time].[Time].");
    }

    private boolean resultMembersContainsExpected(
        String expectedMemberName, List<Member> members)
    {
        for (Member member : members) {
            if (member.getUniqueName().equals(expectedMemberName)) {
                return true;
            }
        }

        return false;
    }

    private void compareColumns() {
        Axis[] axes = result.getAxes();

        NodeList columnList = xmlRoot.getElementsByTagName("columns");

        if (axes.length >= 1) {
            compareTuples("Column", columnList, axes[0].getPositions());
        } else {
            Assert.assertTrue(
                "Must be no columns", columnList.getLength() == 0);
        }
    }

    private void compareRows() {
        Axis[] axes = result.getAxes();

        NodeList rowList = xmlRoot.getElementsByTagName("rows");

        switch (axes.length) {
        case 0:
        case 1:
            Assert.assertTrue("Must be no rows", rowList.getLength() == 0);
            break;

        case 2:
            compareTuples("Row", rowList, axes[1].getPositions());
            break;

        default:
            Assert.fail(
                "Too many axes returned. "
                + "Expected 0, 1 or 2 but got "
                + axes.length);
            break;
        }
    }

    private void _failNotEquals(
        String message, Object expected, Object actual)
    {
        if (message != null) {
            message += "; ";
        } else {
            message = "";
        }
        message += "; expected="
                   + expected
                   + "; actual="
                   + actual
                   + Util.nl
                   + "Query: "
                   + Util.unparse(result.getQuery())
                   + Util.nl;
        TestContext.assertEqualsVerbose(
            TestContext.fold(
                XmlUtility.toString(xmlRoot)),
            toString(result),
            false,
            message);
    }

    private String toString(Result result) {
        Element element = toXml(result);
        return XmlUtility.toString(element);
    }

    private Element toXml(Result result) {
        DocumentBuilder db = XmlUtility.createDomParser(
            false, true, false, new XmlUtility.UtilityErrorHandler());
        final Document document = db.newDocument();
        final Element dataResultXml = document.createElement("dataResult");
        slicerAxisToXml(document, dataResultXml, result);
        final Axis[] axes = result.getAxes();
        for (int i = 0; i < axes.length; i++) {
            Axis axis = axes[i];
            String axisName =
                AxisOrdinal.StandardAxisOrdinal.forLogicalOrdinal(i).name()
                    .toLowerCase();
            axisToXml(document, dataResultXml, axis, axisName);
        }
        final Element dataXml = document.createElement("data");
        dataResultXml.appendChild(dataXml);
        final int axisCount = result.getAxes().length;
        int[] pos = new int[axisCount];
        cellsToXml(document, result, dataXml, pos, axisCount - 1);
        return dataResultXml;
    }

    private void cellsToXml(
        Document document,
        Result result,
        Element parentXml,
        int[] pos,
        int axisOrdinal)
    {
        Axis axis = result.getAxes()[axisOrdinal];
        for (int i = 0; i < axis.getPositions().size(); i++) {
            pos[axisOrdinal] = i;
            if (axisOrdinal == 0) {
                Cell cell = result.getCell(pos);
                final Element cellXml = document.createElement("cell");
                parentXml.appendChild(cellXml);
                final Text textXml =
                    document.createTextNode(String.valueOf(cell.getValue()));
                cellXml.appendChild(textXml);
            } else {
                final Element drowXml = document.createElement("drow");
                parentXml.appendChild(drowXml);
                cellsToXml(document, result, drowXml, pos, axisOrdinal - 1);
            }
        }
    }

    private void slicerAxisToXml(
        final Document document,
        final Element dataResultXml,
        final Result result)
    {
        final List<? extends Dimension> dimensions =
            result.getQuery().getCube().getDimensionList();
        String axisName = "slicer";
        final Axis slicerAxis = result.getSlicerAxis();
        final Element axisXml = document.createElement(axisName);
        dataResultXml.appendChild(axisXml);
        final Element dimensionsXml = document.createElement("dimensions");
        axisXml.appendChild(dimensionsXml);
        final Element tuplesXml = document.createElement("tuples");
        axisXml.appendChild(tuplesXml);
        final Element tupleXml = document.createElement("tuple");
        tuplesXml.appendChild(tupleXml);
        for (Dimension dimension : dimensions) {
            Member member = findSlicerAxisMember(result, dimension);
            if (member == null) {
                continue;
            }
            // Append to the <dimensions> element
            final Element dimXml = document.createElement("dim");
            dimensionsXml.appendChild(dimXml);
            final Text textXml = document.createTextNode(
                member.getDimension().getUniqueName());
            dimXml.appendChild(textXml);

            // Append to the <tuple> element.
            final Element memberXml = document.createElement("member");
            tupleXml.appendChild(memberXml);
            final Text memberTextXml = document.createTextNode(
                member.getUniqueName());
            memberXml.appendChild(memberTextXml);
        }
    }

    /**
     * Returns which member of a given dimension appears in the slicer
     * axis.<p/>
     * <p/>
     * If the dimension occurs on one of the other axes, the answer is null.
     * If the dimension occurs in the slicer axis, the answer is that member.
     * Otherwise it is the default member of the dimension.
     */
    private Member findSlicerAxisMember(Result result, Dimension dimension) {
        final Axis slicerAxis = result.getSlicerAxis();
        if (slicerAxis != null && slicerAxis.getPositions().size() == 1) {
            final List<Member> members = slicerAxis.getPositions().get(0);
            for (Member member : members) {
                if (member.getDimension() == dimension) {
                    return member;
                }
            }
        }
        final Axis[] axes = result.getAxes();
        for (Axis axis : axes) {
            if (axis.getPositions().size() > 0) {
                final List<Member> members = axis.getPositions().get(0);
                for (Member member : members) {
                    if (member.getDimension() == dimension) {
                        // Dimension occurs on non-slicer axis, so it should
                        // not appear in the slicer.
                        return null;
                    }
                }
            }
        }
        return dimension.getHierarchyList().get(0).getDefaultMember();
    }

    private void axisToXml(
        final Document document,
        final Element dataResultXml,
        final Axis axis,
        final String axisName)
    {
        final Element axisXml = document.createElement(axisName);
        dataResultXml.appendChild(axisXml);
        if (axis.getPositions().size() > 0) {
            final Element dimensionsXml = document.createElement("dimensions");
            axisXml.appendChild(dimensionsXml);
            final Position position0 = axis.getPositions().get(0);
            for (Member member : position0) {
                final Element dimXml = document.createElement("dim");
                dimensionsXml.appendChild(dimXml);
                final Text textXml = document.createTextNode(
                    member.getDimension().getUniqueName());
                dimXml.appendChild(textXml);
            }
            final Element tuplesXml = document.createElement("tuples");
            axisXml.appendChild(tuplesXml);
            for (Position position : axis.getPositions()) {
                final Element tupleXml = document.createElement("tuple");
                tuplesXml.appendChild(tupleXml);
                for (final Member member : position) {
                    final Element memberXml = document.createElement("member");
                    tupleXml.appendChild(memberXml);
                    final Text textXml = document.createTextNode(
                        member.getUniqueName());
                    memberXml.appendChild(textXml);
                }
            }
        }
    }


    private void _assertEquals(String message, int expected, int actual) {
        if (expected != actual) {
            _failNotEquals(message, expected, actual);
        }
    }

    private void _assertEquals(
        String message, Object expected, Object actual)
    {
        if (expected == null) {
            if (actual == null) {
                return;
            }
        } else {
            if (expected.equals(actual)) {
                return;
            }
        }
        _failNotEquals(message, expected, actual);
    }

    private void _assertEquals(
        String message, double expected, double actual, double delta)
    {
        if (Double.isInfinite(expected)) {
            if (!(expected == actual)) {
                _failNotEquals(message, expected, actual);
            }
        } else if (!(Math.abs(expected - actual) <= delta)) {
            // Because comparison with NaN always returns false
            _failNotEquals(message, expected, actual);
        }
    }

    private void compareTuples(
        String message, NodeList axisValues, List<Position> resultTuples)
    {
        NodeList expectedTuples = null;
        NodeList expectedDims = null;

        if (axisValues.getLength() != 0) {
            Element axisNode = (Element) axisValues.item(0);
            final Element dims =
                (Element) axisNode.getElementsByTagName("dimensions").item(0);
            expectedDims = dims.getElementsByTagName("dim");
            final Element tuples =
                (Element) axisNode.getElementsByTagName("tuples").item(0);
            expectedTuples = tuples.getElementsByTagName("tuple");
        }

        int numExpectedTuples = expectedTuples == null
            ? 0
            : expectedTuples.getLength();
        _assertEquals(
            message + " number of tuples",
            numExpectedTuples,
            resultTuples.size());

        if (numExpectedTuples != 0) {
            _assertEquals(
                "Invalid test case. Number of dimensions does not match tuple lengths",
                expectedDims.getLength(),
                ((Element) expectedTuples.item(0))
                    .getElementsByTagName("member").getLength());
        }

        for (int idx = 0; idx < numExpectedTuples; idx++) {
            compareTuple(
                message + " tuple " + idx,
                (Element) expectedTuples.item(idx),
                resultTuples.get(idx));
        }
    }

    private void compareTuple(
        String message, Element expectedTuple, Position resultTuple)
    {
        // expectedTuple is a <tuple> definition, containing members
        NodeList expectedMembers = expectedTuple.getElementsByTagName("member");
        int numExpectedMembers = expectedMembers.getLength();

        _assertEquals(
            message + " number of members",
            numExpectedMembers,
            resultTuple.size());

        for (int idx = 0; idx < numExpectedMembers; idx++) {
            String resultName = resultTuple.get(idx).getUniqueName();
            String expectedName =
                expectedMembers.item(idx).getFirstChild().getNodeValue();
            expectedName = foo(expectedName);

            _assertEquals(message + " member " + idx, expectedName, resultName);
        }
    }

    private void compareData() {
        Element dataElement =
            (Element) xmlRoot.getElementsByTagName("data").item(0);
        NodeList expectedRows = dataElement.getElementsByTagName("drow");
        Axis[] axes = result.getAxes();
        int numAxes = axes.length;

        switch (numAxes) {
        case 0:
            compareZeroAxes(expectedRows);
            break;

        case 1:
            compareColumnsOnly(expectedRows, axes);
            break;

        case 2:
            compareRowsAndColumns(expectedRows, axes);
            break;
        }
    }


    private void compareZeroAxes(NodeList expectedRow) {
        int numRows = expectedRow.getLength();

        _assertEquals("Unexpected number of rows", 1, numRows);

        NodeList cellList =
            ((Element) expectedRow.item(0)).getElementsByTagName("cell");

        int numColumns = cellList.getLength();

        _assertEquals("Unexpected number of columns", numColumns, 1);

        int[] coord = new int[0];
        Cell cell = result.getCell(coord);

        String expectedValue = cellList.item(0).getFirstChild().getNodeValue();
        compareCell(coord, expectedValue, cell);
    }

    private void compareColumnsOnly(NodeList expectedRow, Axis[] axes) {
        int numRows = expectedRow.getLength();

        _assertEquals("Unexpected number of rows", 1, numRows);

        NodeList cellList =
            ((Element) expectedRow.item(0)).getElementsByTagName("cell");

        int numColumns = cellList.getLength();

        _assertEquals(
            "Unexpected number of columns",
            numColumns,
            axes[0].getPositions().size());

        int[] coord = new int[1];

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            coord[0] = colIdx;
            Cell cell = result.getCell(coord);

            String expectedValue =
                cellList.item(colIdx).getFirstChild().getNodeValue();
            compareCell(coord, expectedValue, cell);
        }
    }

    private void compareRowsAndColumns(NodeList expectedRows, Axis[] axes) {
        int numRows = expectedRows.getLength();
        int[] coord = new int[2];

        _assertEquals(
            "Number of row tuples must match",
            numRows,
            axes[1].getPositions().size());

        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            Element drow = (Element) expectedRows.item(rowIdx);
            NodeList cellList = drow.getElementsByTagName("cell");

            if (rowIdx == 0) {
                _assertEquals(
                    "Number of data columns: ",
                    cellList.getLength(),
                    axes[0].getPositions().size());
            }

            coord[1] = rowIdx;

            for (int colIdx = 0; colIdx < axes[0].getPositions()
                .size(); colIdx++)
            {
                coord[0] = colIdx;

                Cell cell = result.getCell(coord);

                String expectedValue =
                    cellList.item(colIdx).getFirstChild().getNodeValue();
                compareCell(coord, expectedValue, cell);
            }
        }
    }

    private void compareCell(int[] coord, String expectedValue, Cell cell) {
        if (expectedValue.equalsIgnoreCase("#Missing")) {
            if (!cell.isNull()) {
                _failNotEquals(
                    getErrorMessage(
                        "Expected missing value but got "
                        + cell.getValue()
                        + " at ", coord), null, null);
            }
        } else if (cell.getValue() instanceof Number) {
            Number cellValue = (Number) cell.getValue();
            double expectedDouble = Double.parseDouble(expectedValue);

            _assertEquals(
                getErrorMessage("Values don't match at ", coord),
                expectedDouble,
                cellValue.doubleValue(),
                0.001);
        } else {
            _assertEquals(
                getErrorMessage("Values don't match at ", coord),
                expectedValue,
                cell.getValue());
        }
    }

    private String getErrorMessage(String s, int[] coord) {
        StringBuilder errorAddr = new StringBuilder();
        errorAddr.append(s);

        errorAddr.append(" (");
        for (int idx = 0; idx < coord.length; idx++) {
            if (idx != 0) {
                errorAddr.append(", ");
            }
            errorAddr.append(coord[idx]);
        }
        errorAddr.append(')');
        return errorAddr.toString();
    }
}

// End ResultComparator.java
