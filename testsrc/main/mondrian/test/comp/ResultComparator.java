/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Julian Hyde and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test.comp;

import java.util.HashSet;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import mondrian.olap.*;
import junit.framework.Assert;

/**
 * Compares the {@link Result} produced by a query with the expected result
 * read from an XML file.
 *
 * @see ResultComparatorTest
 */
class ResultComparator {
    private final Element mXMLRoot;
    private final Result mResult;

    public ResultComparator(Element dataResultElement, Result mondrianResult) {
        this.mXMLRoot = dataResultElement;
        this.mResult = mondrianResult;
    }

    public void compareResults() {
        compareSlicers();
        compareColumns();
        compareRows();
        compareData();
    }

    private void compareSlicers() {
        NodeList slicerList = mXMLRoot.getElementsByTagName("slicer");

        Cube cube = mResult.getQuery().getCube();
        Dimension[] dims = cube.getDimensions();
        HashSet defaultDimMembers = new HashSet();

        for (int idx = 0; idx < dims.length; idx++) {

            String uniqueName = dims[idx].getHierarchies()[0].
                    getDefaultMember().getUniqueName();
            defaultDimMembers.add(uniqueName);
        }

        Axis slicerAxis = mResult.getSlicerAxis();
        Member[] members = slicerAxis.positions[0].members;

        Element slicerTuple = (Element) slicerList.item(0);
        if (slicerTuple == null) {
            Assert.assertEquals("Expected no slicers", 0, members.length);
            return;
        }

        final Element tuples = (Element)
            slicerTuple.getElementsByTagName("tuples").item(0);
        final Element tuple = (Element)
            tuples.getElementsByTagName("tuple").item(0);
        NodeList expectedTuple = tuple.getElementsByTagName("member");

        // For each of the expected members, make sure that it's either in the
        // result members[] array or the default member for the dimension.
        int numMembers = expectedTuple.getLength();
        int seenMembers = 0;

        for (int idx = 0; idx < numMembers; idx++) {
            String expectedMemberName =
                expectedTuple.item(idx).getFirstChild().getNodeValue();
            if (resultMembersContainsExpected(expectedMemberName, members)) {
                seenMembers++;
            } else if (defaultDimMembers.contains(expectedMemberName)) {
            } else {
                Assert.fail("Missing slicer: " + expectedMemberName);
            }
        }

        Assert.assertEquals(
            "The query returned more slicer members than were expected",
            members.length, seenMembers);
    }

    private boolean resultMembersContainsExpected(String expectedMemberName,
            Member[] members) {
        for (int idx = 0; idx < members.length; idx++) {
            if (members[idx].getUniqueName().equals(expectedMemberName)) {
                return true;
            }
        }

        return false;
    }

    private void compareColumns() {
        Axis[] axes = mResult.getAxes();

        NodeList columnList = mXMLRoot.getElementsByTagName("columns");

        if (axes.length >= 1) {
            compareTuples("Column", columnList, axes[0].positions);
        } else {
            Assert.assertTrue("Must be no columns",
                columnList.getLength() == 0);
        }
    }

    private void compareRows() {
        Axis[] axes = mResult.getAxes();

        NodeList rowList = mXMLRoot.getElementsByTagName("rows");

        switch (axes.length) {
            case 0:
            case 1:
                Assert.assertTrue("Must be no rows", rowList.getLength() == 0);
                break;

            case 2:
                compareTuples("Row", rowList, axes[1].positions);
                break;

            default:
                Assert.fail("Too many axes returned. " +
                    "Expected 0, 1 or 2 but got " + axes.length);
                break;
        }
    }

    private void compareTuples(String message, NodeList axisValues,
            Position[] resultTuples) {
        NodeList expectedTuples = null;
        NodeList expectedDims = null;

        if (axisValues.getLength() != 0) {
            Element axisNode = (Element) axisValues.item(0);
            final Element dims = (Element)
                axisNode.getElementsByTagName("dimensions").item(0);
            expectedDims = dims.getElementsByTagName("dim");
            final Element tuples = (Element)
                axisNode.getElementsByTagName("tuples").item(0);
            expectedTuples = tuples.getElementsByTagName("tuple");
        }

        int numExpectedTuples = expectedTuples == null ? 0 : expectedTuples.getLength();
        Assert.assertEquals(message + " number of tuples", numExpectedTuples,
                resultTuples.length);

        if (numExpectedTuples != 0) {
            Assert.assertEquals("Invalid test case. Number of dimensions does not match tuple lengths",
                    expectedDims.getLength(), ((Element)expectedTuples.item(0)).getElementsByTagName("member").getLength());
        }

        for (int idx = 0; idx < numExpectedTuples; idx++) {
            compareTuple(message + " tuple " + idx, (Element) expectedTuples.item(idx), resultTuples[idx]);
        }
    }

    private void compareTuple(String message, Element expectedTuple, Position resultTuple) {
        // expectedTuple is a <tuple> definition, containing members
        NodeList expectedMembers = expectedTuple.getElementsByTagName("member");
        int numExpectedMembers = expectedMembers.getLength();

        Assert.assertEquals(message + " number of members", numExpectedMembers, resultTuple.members.length);

        for (int idx = 0; idx < numExpectedMembers; idx++) {
            String resultName = resultTuple.members[idx].getUniqueName();
            String expectedName = expectedMembers.item(idx).getFirstChild().getNodeValue();

            Assert.assertEquals(message + " member " + idx, expectedName,
                    resultName);
        }
    }

    private void compareData() {
        Element dataElement = (Element) mXMLRoot.getElementsByTagName("data").item(0);
        NodeList expectedRows = dataElement.getElementsByTagName("drow");
        Axis[] axes = mResult.getAxes();
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

        Assert.assertEquals("Unexpected number of rows", 1, numRows);

        NodeList cellList = ((Element)expectedRow.item(0)).getElementsByTagName("cell");

        int numColumns = cellList.getLength();

        Assert.assertEquals("Unexpected number of columns", numColumns, 1);

        int[] coord = new int[0];
        Cell cell = mResult.getCell(coord);

        String expectedValue = cellList.item(0).getFirstChild().getNodeValue();
        compareCell(coord, expectedValue, cell);
    }

    private void compareColumnsOnly(NodeList expectedRow, Axis[] axes) {
        int numRows = expectedRow.getLength();

        Assert.assertEquals("Unexpected number of rows", 1, numRows);

        NodeList cellList = ((Element) expectedRow.item(0)).getElementsByTagName("cell");

        int numColumns = cellList.getLength();

        Assert.assertEquals("Unexpected number of columns", numColumns, axes[0].positions.length);

        int[] coord = new int[1];

        for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            coord[0] = colIdx;
            Cell cell = mResult.getCell(coord);

            String expectedValue = cellList.item(colIdx).getFirstChild().getNodeValue();
            compareCell(coord, expectedValue, cell);
        }

        return;
    }

    private void compareRowsAndColumns(NodeList expectedRows, Axis[] axes) {
        int numRows = expectedRows.getLength();
        int[] coord = new int[2];

        Assert.assertEquals("Number of row tuples must match", numRows,
                axes[1].positions.length);

        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            Element drow = (Element) expectedRows.item(rowIdx);
            NodeList cellList = drow.getElementsByTagName("cell");

            if (rowIdx == 0) {
                Assert.assertEquals("Number of data columns: ",
                    cellList.getLength(), axes[0].positions.length);
            }

            coord[1] = rowIdx;

            for (int colIdx = 0; colIdx < axes[0].positions.length; colIdx++) {
                coord[0] = colIdx;

                Cell cell = mResult.getCell(coord);

                String expectedValue = cellList.item(colIdx).getFirstChild().getNodeValue();
                compareCell(coord, expectedValue, cell);
            }
        }
    }

    private void compareCell(int[] coord, String expectedValue, Cell cell) {
        if (expectedValue.equalsIgnoreCase("#Missing")) {
            if (!cell.isNull()) {
                Assert.fail(getErrorMessage("Expected missing value but got " + cell.getValue()
                        + " at ", coord));
            }
        }

        else if (cell.getValue() instanceof Number) {
            Number cellValue = (Number) cell.getValue();
            double expectedDouble = Double.parseDouble(expectedValue);

            Assert.assertEquals(getErrorMessage("Values don't match at ", coord),
                    expectedDouble, cellValue.doubleValue(), 0.001);
        }

        else {
            Assert.assertEquals(getErrorMessage("Values don't match at ", coord),
                    expectedValue, cell.getValue());
        }
    }

    private String getErrorMessage(String s, int[] coord) {
        StringBuffer errorAddr = new StringBuffer();
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
