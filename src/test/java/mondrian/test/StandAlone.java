/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.*;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;

public class StandAlone {
    private static final String[] indents = {
        "    ", "        ", "            ", "                "
    };

    private static Connection cxn;

    private static String cellProp;
    private static boolean printMemberProps = false;

    private static BufferedReader stdin =
        new BufferedReader(new InputStreamReader(System.in));

    public static final String ConnectionString =
        "Provider=mondrian;"
        + "Jdbc=jdbc:JSQLConnect://engdb04:1433/database=MondrianFoodmart/user=mondrian/password=password;"
        + "Catalog=file:demo\\FoodMart.mondrian.xml;"
        + "JdbcDrivers=com.jnetdirect.jsql.JSQLDriver;";

    public static void main(String[] args) {
        long now = System.currentTimeMillis();
//        java.sql.DriverManager.setLogWriter(new PrintWriter(System.err));

        cxn = DriverManager.getConnection(ConnectionString, null);

        System.out.println(
            "Connected in " + (System.currentTimeMillis() - now) + " usec");
        processCommands();
    }

    private static void processCommands() {
        BufferedReader in =
            new BufferedReader(new InputStreamReader(System.in));
        long startTime = System.currentTimeMillis();

        inputLoop:
        for (; ;) {
            try {
                String line = in.readLine();
                if (line == null) {
                    break inputLoop;
                }

                if (line.equals("\\q")) {
                    break inputLoop;
                } else if (line.startsWith("\\")) {
                    processSlashCommand(line);
                } else {
                    StringBuilder buf = new StringBuilder();
                    buf.append(line);

                    for (;;) {
                        System.out.print("> ");
                        line = in.readLine();
                        if (line == null) {
                            break inputLoop;
                        }
                        if (line.equals(".")) {
                            break;
                        }

                        buf.append(' ');
                        buf.append(line);
                    }

                    long queryStart = System.currentTimeMillis();

                    String queryString = buf.toString();
                    boolean printResults = false;
                    if (buf.substring(0, 1).equals("-")) {
                        queryString = buf.substring(1);
                        printResults = true;
                    }

                    Query query = cxn.parseQuery(queryString);
                    Result result = cxn.execute(query);
                    displayElapsedTime(queryStart, "Elapsed time");

                    printResult(result, printResults);
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        displayElapsedTime(startTime, "Connect time");
    }

    private static void displayElapsedTime(long startTime, String message) {
        long elapsed = System.currentTimeMillis() - startTime;
        int seconds, msecs;

        msecs = (int) (elapsed % 1000);
        seconds = (int) (elapsed / 1000);

        System.out.println(
            MessageFormat.format(
                "{2}: {0,number,0}.{1,number,000} ({3})",
                seconds, msecs, message, elapsed));
    }

    private static void printResult(Result result, boolean outputResults) {
        Axis slicer = result.getSlicerAxis();
        int nonNullCellCount = 0;
        int cellCount = 0;
        int numRows = 0;
        int numColumns;

        List<Position> slicerpositions = slicer.getPositions();

        int numSlicers = slicer.getPositions().size();

        if (numSlicers > 0 && outputResults) {
            System.out.print("Slicers: {");
            for (Position pos : slicerpositions) {
                printMembers(pos);
            }

            System.out.println("}");
        }

        Axis[] axes = result.getAxes();
        if (axes.length == 0) {
            numColumns = 0;
            cellCount = 1;

            if (outputResults) {
                System.out.println("No axes.");
                Cell cell = result.getCell(new int[0]);
                printCell(cell);
            }
        } else if (axes.length == 1) {
            // Only columns
            List<Position> cols = axes[0].getPositions();

            numColumns = cols.size();

            for (int idx = 0; idx < cols.size(); idx++) {
                Position col = cols.get(idx);

                if (outputResults) {
                    System.out.print("Column " + idx + ": ");
                    printMembers(col);
                }


                Cell cell = result.getCell(new int[]{idx});

                if (!cell.isNull()) {
                    nonNullCellCount++;
                }

                cellCount++;

                if (outputResults) {
                    printCell(cell);
                }
            }
        } else {
            List<Position> colPositions = axes[0].getPositions();
            List<Position> rowPositions = axes[1].getPositions();

            numColumns = colPositions.size();
            numRows = rowPositions.size();

            int[] coords = new int[2];

            if (outputResults) {
                System.out.println("Column tuples: ");
            }

            for (int colIdx = 0; colIdx < colPositions.size(); colIdx++) {
                Position col = colPositions.get(colIdx);

                if (outputResults) {
                    System.out.print("Column " + colIdx + ": ");
                    printMembers(col);
                    System.out.println();
                }

                for (int rowIdx = 0; rowIdx < rowPositions.size(); rowIdx++) {
                    if (outputResults) {
                        System.out.print("(" + colIdx + ", " + rowIdx + ") ");
                        printMembers(rowPositions.get(rowIdx));
                        System.out.print("} = ");
                    }

                    coords[0] = colIdx;
                    coords[1] = rowIdx;

                    Cell cell = result.getCell(coords);

                    if (!cell.isNull()) {
                        nonNullCellCount++;
                    }

                    cellCount++;

                    if (outputResults) {
                        printCell(cell);
                    }
                }
            }
        }

        System.out.println("cellCount: " + cellCount);
        System.out.println("nonNullCellCount: " + nonNullCellCount);
        System.out.println("numSlicers: " + numSlicers);
        System.out.println("numColumns: " + numColumns);
        System.out.println("numRows: " + numRows);
    }

    private static void printCell(Cell cell) {
        Object cellPropValue;

        if (cellProp != null) {
            cellPropValue = cell.getPropertyValue(cellProp);
            System.out.print("(" + cellPropValue + ")");
        }

        System.out.println(cell.getFormattedValue());
    }

    private static void printMembers(Position pos) {
        boolean needComma = false;

        for (Member member : pos) {
            if (needComma) {
                System.out.print(',');
            }
            needComma = true;

            System.out.print(member.getUniqueName());

            if (printMemberProps) {
                Property[] props = member.getProperties();

                if (props.length > 0) {
                    System.out.print(" {");
                    for (int idx = 0; idx < props.length; idx++) {
                        if (idx > 1) {
                            System.out.print(", ");
                        }
                        Property prop = props[idx];

                        System.out.print(
                            prop.getName() + ": "
                            + member.getPropertyValue(prop));
                    }
                    System.out.print("}");
                }
            }
        }
    }

    private static void processSlashCommand(String line) throws IOException {
        if (line.equals("\\schema")) {
            printSchema(cxn.getSchema());
        } else if (line.equals("\\dbg")) {
            PrintWriter out = java.sql.DriverManager.getLogWriter();

            if (out == null) {
                java.sql.DriverManager.setLogWriter(
                    new PrintWriter(System.err));
                System.out.println("SQL driver logging enabled");
            } else {
                java.sql.DriverManager.setLogWriter(null);
                System.out.println("SQL driver logging disabled");
            }

            cxn.close();
            cxn = DriverManager.getConnection(ConnectionString, null);
        } else if (line.equals("\\cp")) {
            System.out.print("Enter cell property: ");
            cellProp = stdin.readLine();
            if (cellProp == null || cellProp.length() == 0) {
                cellProp = null;
            }
        } else if (line.equals("\\mp")) {
            printMemberProps ^= true;
            System.out.println("Print member properties: " + printMemberProps);
        } else if (line.startsWith("\\test ")) {
            StringTokenizer st = new StringTokenizer(line, " ", false);
            st.nextToken(); // throw away /test
            String threads = st.nextToken();
            String seconds = st.nextToken();
            String useRandom;

            try {
                useRandom = st.nextToken();
            } catch (NoSuchElementException nse) {
                useRandom = "false";
            }

            try {
                runTest(
                    Integer.parseInt(threads),
                    Integer.parseInt(seconds),
                    Boolean.valueOf(useRandom));
            } catch (NumberFormatException nfe) {
                System.out.println(
                    "Please enter a valid integer for the number of threads "
                    + "and the execution time");
            }
        } else {
            System.out.println("Commands:");
            System.out.println("\t\\q        Quit");
            System.out.println("\t\\schema   Print the schema");
            System.out.println("\t\\dbg      Toggle SQL driver debugging");
        }
    }

    private static void runTest(
        int numThreads, int seconds, boolean randomQueries)
    {
        QueryRunner[] runners = new QueryRunner[numThreads];

        System.out.println(
            "Running multi-threading test with " + numThreads
            + " threads for " + seconds + " seconds.");
        System.out.println(
            "Queries will " + (randomQueries ? "" : "not ") + "be random.");

        for (int idx = 0; idx < runners.length; idx++) {
            runners[idx] = new QueryRunner(idx, seconds, randomQueries);
        }

        for (QueryRunner runner : runners) {
            runner.start();
        }

        for (QueryRunner runner : runners) {
            try {
                runner.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        for (QueryRunner runner : runners) {
            runner.report(System.out);
        }
    }

    private static void printSchema(Schema schema) {
        Cube[] cubes = schema.getCubes();
        Dimension[] dimensions = schema.getSharedDimensions();

        System.out.println(
            "Schema: " + schema.getName() + " "
            + cubes.length + " cubes and "
            + dimensions.length + " shared hierarchies");

        System.out.println("---Cubes ");
        for (int idx = 0; idx < cubes.length; idx++) {
            printCube(cubes[idx]);
            System.out.println("-------------------------------------------");
        }

        System.out.println("---Shared hierarchies");
        for (int idx = 0; idx < dimensions.length; idx++) {
            printDimension(dimensions[idx]);
        }
    }

    private static void printCube(Cube cube) {
        System.out.println("Cube " + cube.getName());

        for (Dimension dim : cube.getDimensionList()) {
            printDimension(dim);
        }
    }

    private static void printDimension(Dimension dim) {
        System.out.println(
            "\tDimension " + dim.getName()
            + " type: " + dim.getDimensionType().name());

        System.out.println("\t    Description: " + dim.getDescription());

        for (Hierarchy hierarchy : dim.getHierarchyList()) {
            printHierarchy(1, hierarchy);
        }
    }


    private static void printHierarchy(int indent, Hierarchy hierarchy) {
        String indentString = indents[indent];

        System.out.println(indentString + " Hierarchy " + hierarchy.getName());
        System.out.println(
            indentString + "    Description: " + hierarchy.getDescription());
        System.out.println(
            indentString + "    Default member: "
            + hierarchy.getDefaultMember().getUniqueName());

        for (Level level : hierarchy.getLevelList()) {
            printLevel(indent + 1, level);
        }
    }

    private static void printLevel(int indent, Level level) {
        String indentString = indents[indent];

        System.out.println(indentString + "Level " + level.getName());

        System.out.print(level.getUniqueName());
    }
}

// End StandAlone.java
