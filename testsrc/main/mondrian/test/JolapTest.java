/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.test;

import junit.framework.TestCase;
import junit.textui.TestRunner;
import mondrian.olap.Util;
import org.omg.java.cwm.objectmodel.core.Attribute;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.olap.OLAPException;
import javax.olap.cursor.CubeCursor;
import javax.olap.cursor.DimensionCursor;
import javax.olap.cursor.EdgeCursor;
import javax.olap.metadata.*;
import javax.olap.query.dimensionfilters.*;
import javax.olap.query.enumerations.*;
import javax.olap.query.querycoremodel.*;
import javax.olap.resource.Connection;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

/**
 * Tests Mondrian's compliance with the JOLAP API.
 *
 * @author jhyde
 * @since Dec 23, 2002
 * @version $Id$
 **/
public class JolapTest extends TestCase {
    private static final String nl = System.getProperty("line.separator");

    public JolapTest(String name) {
        super(name);
    }

    public void testConnect() throws OLAPException {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        doConnect(pw);
        pw.flush();
        assertEquals("Dimension name is Store Size in SQFT" + nl +
                "Dimension name is Store" + nl +
                "Dimension name is Time" + nl +
                "Dimension name is Product" + nl +
                "Dimension name is Store Type" + nl +
                "Dimension name is Warehouse" + nl,
                sw.toString());
    }

    /**
     * Adapted from spec version 0.85, section 6.3.
     */
    private void doConnect(PrintWriter out) throws OLAPException {
        Connection cx = getConnection();
        // Get the list of all JOLAP Dimensions for the default Schema
        // and print out their names...
        Collection dimList = cx.getDimensions();
        Iterator dimIter = dimList.iterator();
        while ( dimIter.hasNext() )
        {
            javax.olap.metadata.Dimension myDim
                    = (javax.olap.metadata.Dimension)dimIter.next();
            out.println( "Dimension name is " + myDim.getName() );
        }
        // Now close the connection to the OLAP resource
        cx.close();
    }

    private Connection getConnection() throws OLAPException {
        try {
            Hashtable env = new Hashtable();
//          final String factoryClassName = "com.mycompany.javax.olap.resource.ConnectionFactoryImpl";
            String factoryClassName = mondrian.jolap.MondrianJolapConnectionFactory.class.getName();
            env.put( Context.INITIAL_CONTEXT_FACTORY,
                    factoryClassName );
            Context initCtx = new InitialContext( env );
            // Obtain the JOLAP MondrianJolapConnectionFactory from the JNDI tree
            javax.olap.resource.ConnectionFactory cxf
                    = (javax.olap.resource.ConnectionFactory)initCtx.lookup(
                            "JOLAPServer" );
            // Create a connection spec
            javax.olap.resource.ConnectionSpec cxs = cxf.createConnectionSpec();
            setProperty(cxs, "name", "jolapuser");
            setProperty(cxs, "password", "guest");
            // Note: if the specific type of ConnectionSpec is not known,
            // clients can introspect the returned instance to determine
            // which JavaBean-compliant attributes are required.
            // Establish a connection to the OLAP resource
            Connection cx = (Connection)cxf.getConnection( cxs );
            return cx;
        } catch (NamingException e) {
            throw Util.newError(e, "Error while making connection");
        }
    }

    private static void setProperty(Object cxs, String propertyName, Object propertyValue) {
        try {
            final Method method = cxs.getClass().getMethod("get" + propertyName.substring(0,1).toUpperCase() + propertyName.substring(1),
                    new Class[] {String.class});
            method.invoke(cxs, new Object[] {propertyValue});
        } catch (NoSuchMethodException e) {
            throw Util.newError(e, "Error while setting property '" + propertyName + "'");
        } catch (SecurityException e) {
            throw Util.newError(e, "Error while setting property '" + propertyName + "'");
        } catch (IllegalArgumentException e) {
            throw Util.newError(e, "Error while setting property '" + propertyName + "'");
        } catch (IllegalAccessException e) {
            throw Util.newError(e, "Error while setting property '" + propertyName + "'");
        } catch (InvocationTargetException e) {
            throw Util.newError(e, "Error while setting property '" + propertyName + "'");
        }
    }

    public void testSimpleCubeView() throws OLAPException {
        Connection connection = getConnection();
        Cube salesCube = getCube(connection, "Sales");
        CubeView query = connection.createCubeView();
        EdgeView rowsEdge = query.createOrdinateEdge();
        EdgeView colsEdge = query.createOrdinateEdge();
        EdgeView pageEdge = query.createPageEdge();
    }

    public void _testSimpleDimensionView() throws OLAPException {
        Connection someConnection = getConnection();
        Dimension prod = getDimension(someConnection, "Product");
        DimensionView products = someConnection.createDimensionView(prod);
        products.setDimension(prod);
        DimensionStepManager steps = products.createDimensionStepManager();
        AttributeFilter nameFilter = (AttributeFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTE_FILTER);
        Attribute nameAttr = (Attribute)prod.getFeature().get(0);
        nameFilter.setAttribute(nameAttr);
        nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
        nameFilter.setOp(OperatorTypeEnum.EQ);
        nameFilter.setRhs( "Fred" );
    }

    public void testSimpleDimensionView() throws OLAPException {
        Connection someConnection = getConnection();
        final DimensionView products = createSimpleDimensionView(someConnection);
        Util.discard(products);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        print(products, pw);
        pw.flush();
        assertEquals("", sw.toString());
    }

    private DimensionView createSimpleDimensionView(Connection someConnection) throws OLAPException {
        Cube salesCube = getCube(someConnection, "Sales");
        Dimension prod = getDimension(salesCube, "Product");
        DimensionView products = someConnection.createDimensionView(prod);
        products.setDimension(prod);
        DimensionStepManager steps = products.createDimensionStepManager();
        AttributeFilter nameFilter = (AttributeFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTE_FILTER);
        Attribute nameAttr = getAttribute(prod, "Product Subcategory");
        nameFilter.setAttribute(nameAttr);
        nameFilter.setSetAction(SetActionTypeEnum.INITIAL);
        nameFilter.setOp(OperatorTypeEnum.EQ);
        nameFilter.setRhs( "Beer" );
        return products;
    }

    private Attribute getAttribute(Dimension prod, String name) {
        for (Iterator features = prod.getFeature().iterator(); features.hasNext();) {
            Object o = features.next();
            if (o instanceof Attribute) {
                Attribute attribute = (Attribute) o;
                if (attribute.getName().equals(name)) {
                    return attribute;
                }
            }
        }
        return null;
    }

    public void testSimpleEdgeViewWithOneDimensionView() throws OLAPException {
        Connection someConnection = getConnection();
        DimensionView products = createSimpleEdgeWithOneDimensionView(someConnection);
        Util.discard(products);
    }

    private DimensionView createSimpleEdgeWithOneDimensionView(Connection someConnection) throws OLAPException {
        Cube salesCube = getCube(someConnection, "Sales");
        Dimension customer = getDimension(salesCube, "Customers");
        DimensionView customerView = someConnection.createDimensionView(customer);
        customerView.setDimension(customer);
        DimensionStepManager steps = customerView.createDimensionStepManager();
        AttributeFilter genderFilter = (AttributeFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTE_FILTER);
        Attribute genderAttr = getAttribute(customer, "Gender");
        genderFilter.setAttribute(genderAttr);
        genderFilter.setSetAction(SetActionTypeEnum.INITIAL);
        genderFilter.setOp(OperatorTypeEnum.EQ);
        genderFilter.setRhs("F");
        CubeView sampleCube = someConnection.createCubeView(); //salesCube
        EdgeView rows = sampleCube.createOrdinateEdge();
        Segment defaultSegment = rows.createSegment();
        defaultSegment.getDimensionStepManager().add( steps );
        return customerView;
    }

    /**
     * "Sample code 8.3: Attribute Filter with Compound Step"
     */
    public void testMultiStepAttributeFilter() throws OLAPException {
        Connection someConnection = getConnection();
        DimensionView multiStep = createMultiStepAttributeFilter(someConnection);
        Util.discard(multiStep);
    }

    private DimensionView createMultiStepAttributeFilter(Connection someConnection) throws OLAPException {
        Dimension prod = getDimension(someConnection, "Product");
        DimensionView multiStep =   someConnection.createDimensionView(prod);
        multiStep.setDimension(prod);
        AttributeReference selectCode = (AttributeReference)
                multiStep.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE);
        Attribute code = (Attribute)prod.getFeature().get(0);
        selectCode.setAttribute(code);
        DimensionStepManager steps = multiStep.createDimensionStepManager();
        AttributeFilter redFilter = (AttributeFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTE_FILTER);
        Attribute color = (Attribute) prod.getFeature().get(1);
        redFilter.setAttribute(color);
        redFilter.setSetAction(SetActionTypeEnum.INITIAL);
        redFilter.setOp(OperatorTypeEnum.EQ);
        redFilter.setRhs("Red");
        AttributeFilter blueFilter = (AttributeFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.ATTRIBUTE_FILTER);
        blueFilter.setAttribute(color);
        blueFilter.setSetAction(SetActionTypeEnum.APPEND);
        blueFilter.setOp(OperatorTypeEnum.EQ);
        blueFilter.setRhs("Blue");
        return multiStep;
    }


    /**
     * "Sample code 8.4: Level Filter"
     */
    public void testLevelFilter() throws OLAPException {
        Connection someConnection = getConnection();
        DimensionView partMembers = createLevelFilter(someConnection);
        Util.discard(partMembers);
    }

    private DimensionView createLevelFilter(Connection someConnection) throws OLAPException {
        Dimension prod = getDimension(someConnection, "Product");
        DimensionView partMembers = someConnection.createDimensionView(prod);
        partMembers.setDimension(prod);
        AttributeReference selectCode = (AttributeReference)
                partMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE);
        Attribute code = (Attribute)prod.getFeature().get(0);
        selectCode.setAttribute(code);
        DimensionStepManager steps = partMembers.createDimensionStepManager();
        LevelFilter levelFilter = (LevelFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.LEVEL_FILTER);
        Collection parts = prod.getMemberSelection();
        Level part = (Level)parts.iterator().next();
        levelFilter.setLevel(part);
        levelFilter.setSetAction(SetActionTypeEnum.INITIAL);
        return partMembers;
    }

    private Dimension getDimension(Connection someConnection, String name) throws OLAPException {
        final Collection dimensions = someConnection.getDimensions();
        for (Iterator iterator = dimensions.iterator(); iterator.hasNext();) {
            Dimension dimension = (Dimension) iterator.next();
            if (dimension.getName().equals(name)) {
                return dimension;
            }
        }
        return null;
    }

    /**
     * "Sample code 8.5: Drill Filter"
     */
    public void testDrillFilter() throws OLAPException {
        Connection someConnection = getConnection();
        DimensionView drill = createDrillFilter(someConnection);
        Util.discard(drill);
    }

    private DimensionView createDrillFilter(Connection someConnection) throws OLAPException {
        Dimension prod = getDimension(someConnection, "Product");
        DimensionView drill = someConnection.createDimensionView(prod);
        drill.setDimension(prod);
        AttributeReference selectCode = (AttributeReference)
                drill.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE);
        Attribute code = (Attribute)prod.getFeature().get(0);
        selectCode.setAttribute(code);
        DimensionStepManager steps = drill.createDimensionStepManager();
        HierarchyFilter hierarchyFilter =
        (HierarchyFilter)steps.createDimensionStep(DimensionStepTypeEnum.HIERARCHY_MEMBER_FILTER);
        Collection hierarchies = prod.getHierarchy();
        LevelBasedHierarchy stdHierarchy = (LevelBasedHierarchy)
                hierarchies.iterator().next();
        hierarchyFilter.setHierarchy(stdHierarchy);
        hierarchyFilter.setSetAction(SetActionTypeEnum.INITIAL);
        hierarchyFilter.setHierarchyFilterType(HierarchyFilterTypeEnum.ALL_MEMBERS);
        Drill drillW1000 = (Drill)
                steps.createDimensionStep(DimensionStepTypeEnum.DRILL_FILTER);
        Member W1000 = someConnection.getMemberObjectFactories().createMember(prod);
        drillW1000.setDrillMember(W1000);
        drillW1000.setSetAction(SetActionTypeEnum.APPEND);
        drillW1000.setDrillType(DrillTypeEnum.CHILDREN);
        return drill;
    }

    /**
     * "Sample code 8.6: Data-based Exception Filter"
     */
    public void testDataBasedExceptionFilter() throws OLAPException {
        DimensionView exceptionMembers = createDataBasedExceptionFilter();
        Util.discard(exceptionMembers);
    }

    private DimensionView createDataBasedExceptionFilter() throws OLAPException {
        Connection someConnection = getConnection();
        Dimension prod = getDimension(someConnection, "Product");
        Dimension geog = getDimension(someConnection, "Store");
        Dimension meas = getDimension(someConnection, "Measures");
        Dimension time = getDimension(someConnection, "Time");
        DimensionView exceptionMembers = someConnection.createDimensionView(prod);
        AttributeReference selectCode = (AttributeReference)
                exceptionMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE);
        Attribute code = (Attribute)prod.getFeature().get(0);
        selectCode.setAttribute(code);
        DimensionStepManager steps = exceptionMembers.createDimensionStepManager();
        ExceptionMemberFilter prodsGT500 = (ExceptionMemberFilter)
                steps.createDimensionStep(DimensionStepTypeEnum.EXCEPTION_MEMBER_FILTER);
        prodsGT500.setSetAction(SetActionTypeEnum.INITIAL);
        prodsGT500.setOp(OperatorTypeEnum.EQ);
        prodsGT500.setRhs(new Integer(500));
        QualifiedMemberReference qmr1 = (QualifiedMemberReference)
                prodsGT500.createDataBasedMemberFilterInput(DataBasedMemberFilterInputTypeEnum.QUALIFIED_MEMBER_REFERENCE);
        final MemberObjectFactories memberObjectFactories = someConnection.getMemberObjectFactories();
        Member Sales = memberObjectFactories.createMember(meas);
        Member DavesStore = memberObjectFactories.createMember(geog);
        Member Jan2001 = memberObjectFactories.createMember(time);
        qmr1.getMember().add(Sales);
        qmr1.getMember().add(DavesStore);
        qmr1.getMember().add(Jan2001);
        return exceptionMembers;
    }

    /**
     * "Sample code 8.7: Data-based Ranking Filter"
     */
    public void testDataBasedRankingFilter() throws OLAPException {
        Connection someConnection = getConnection();
        DimensionView rankingMembers = createDataBasedRankingFilter(someConnection);
        Util.discard(rankingMembers);
    }

    private DimensionView createDataBasedRankingFilter(Connection someConnection) throws OLAPException {
        Dimension prod = getDimension(someConnection, "Product");
        Dimension geog = getDimension(someConnection, "Store");
        Dimension meas = getDimension(someConnection, "Measures");
        Dimension time = getDimension(someConnection, "Time");
        DimensionView rankingMembers = someConnection.createDimensionView(prod);
        AttributeReference selectCode = (AttributeReference)
                rankingMembers.createSelectedObject(SelectedObjectTypeEnum.ATTRIBUTE_REFERENCE);
        Attribute code = (Attribute)prod.getFeature().get(0);
        selectCode.setAttribute(code);
        DimensionStepManager steps =
        rankingMembers.createDimensionStepManager();
        RankingMemberFilter TB5Prods =
        (RankingMemberFilter)steps.createDimensionStep(DimensionStepTypeEnum.RANKING_MEMBER_FILTER);
        TB5Prods.setSetAction(SetActionTypeEnum.INITIAL);
        TB5Prods.setType(RankingTypeEnum.TOP_BOTTOM);
        TB5Prods.setTop(5);
        TB5Prods.setBottom(5);
        TB5Prods.setTopPercent(false);
        TB5Prods.setBottomPercent(false);
        QualifiedMemberReference qmr1 = (QualifiedMemberReference)
                TB5Prods.createDataBasedMemberFilterInput(DataBasedMemberFilterInputTypeEnum.QUALIFIED_MEMBER_REFERENCE);
        final MemberObjectFactories memberObjectFactories = someConnection.getMemberObjectFactories();
        Member Sales = memberObjectFactories.createMember(meas);
        Member DavesStore = memberObjectFactories.createMember(geog);
        Member Jan2001 = memberObjectFactories.createMember(time);
        qmr1.getMember().add(Sales);
        qmr1.getMember().add(DavesStore);
        qmr1.getMember().add(Jan2001);
        return rankingMembers;
    }

    /**
     * Code fragment for section 9.2.3.2
     */
    private CubeCursor foo() throws OLAPException {
        // Create a DimensionView for each dimension
        Connection connection = getConnection();
        Cube salesCube = getCube(connection, "Sales");
        final Dimension channel = getDimension(salesCube, "Promotions" /*"Channel"*/);
        DimensionView channelView = connection.createDimensionView(channel);
        final Dimension prod = getDimension(salesCube, "Product");
        DimensionView productView = connection.createDimensionView(prod);
        final Dimension geog = getDimension(salesCube, "Gender" /*"Geography"*/);
        DimensionView geographyView = connection.createDimensionView(geog);
        final Dimension time = getDimension(salesCube, "Marital Status"/*"Time"*/);
        DimensionView timeView = connection.createDimensionView(time);
        final Dimension meas = getDimension(salesCube, "Measures");
        MeasureView measView = (MeasureView) connection.createDimensionView(meas);
        // jhyde added
        CubeView query = connection.createCubeView(); //salesCube
        // Create a columns edge and add the time and geography views
        //EdgeView columns = connection.createEdgeView();
        EdgeView columns = query.createOrdinateEdge();
        columns.getDimensionView().add(timeView);
        columns.getDimensionView().add(geographyView);
        // Create a rows edge and add the product view
        //EdgeView rows = connection.createEdgeView();
        EdgeView rows = query.createOrdinateEdge();
        rows.getDimensionView().add(productView);
        // Create a pages edge and add the channel dimension view
        //EdgeCursor pages = connection.createDimensionView();
        EdgeView pages = query.createPageEdge();
        pages.getDimensionView().add(channelView);
        pages.getDimensionView().add(measView);
        // Create the query cube view and add edges and the measure
        //CubeView query = connection.createCubeView();
        //query.addOrdinateEdge(rows);
        //query.addOrdinateEdge(columns);
        //query.addPageEdge(pages);
        CubeCursor dataCursor = query.createCursor();
        //EdgeCursor pageCursor=(List)dataCursor.getPageEdge().elementAt(0);
        EdgeCursor pageCursor=(EdgeCursor) dataCursor.getPageEdge().iterator().next();
        //EdgeCursor rowCursor=(List)dataCursor.getOrdinateEdge().elementAt(0);
        EdgeCursor columnCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(0);
        EdgeCursor rowCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(1);
        DimensionCursor measureCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(0);
        DimensionCursor channelCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(1);
        DimensionCursor productCursor = (DimensionCursor) rowCursor.getDimensionCursor().get(0);
        DimensionCursor geographyCursor =(DimensionCursor) columnCursor.getDimensionCursor().get(0);
        DimensionCursor timeCursor = (DimensionCursor) columnCursor.getDimensionCursor().get(1);
        columnCursor.setFetchSize(6);
        rowCursor.setFetchSize(4);
        return dataCursor;
    }

    private CubeCursor foo2() throws OLAPException {
        // Create a DimensionView for each dimension
        Connection connection = getConnection();
        Cube salesCube = getCube(connection, "Sales");
        final Dimension chan = getDimension(salesCube, "Promotions" /*"Channel"*/);
        DimensionView channelView = connection.createDimensionView(chan);
        final Dimension prod = getDimension(salesCube, "Product");
        DimensionView productView = connection.createDimensionView(prod);
        final Dimension geog = getDimension(salesCube, "Gender" /*"Geography"*/);
        DimensionView geographyView = connection.createDimensionView(geog);
        final Dimension time = getDimension(salesCube, "Time");
        DimensionView timeView = connection.createDimensionView(time);
        final Dimension meas = getDimension(salesCube, "Measures");
        MeasureView measView = (MeasureView) connection.createDimensionView(meas);
        // jhyde added
        CubeView query = connection.createCubeView();//salesCube
        // Create a columns edge and add the time and geography views
        //EdgeView columns = connection.createEdgeView();
        EdgeView columns = query.createOrdinateEdge();
        columns.getDimensionView().add(timeView);
        columns.getDimensionView().add(geographyView);
        // Create a rows edge and add the product view
        //EdgeView rows = connection.createEdgeView();
        EdgeView rows = query.createOrdinateEdge();
        rows.getDimensionView().add(productView);
        // Create a pages edge and add the channel dimension view
        //EdgeCursor pages = connection.createDimensionView();
        EdgeView pages = query.createPageEdge();
        pages.getDimensionView().add(channelView);
        pages.getDimensionView().add(measView);
        // Create the query cube view and add edges and the measure
        //CubeView query = connection.createCubeView();
        //query.addOrdinateEdge(rows);
        //query.addOrdinateEdge(columns);
        //query.addPageEdge(pages);
        CubeCursor dataCursor = query.createCursor();
        //EdgeCursor pageCursor=(List)dataCursor.getPageEdge().elementAt(0);
        EdgeCursor pageCursor=(EdgeCursor) dataCursor.getPageEdge().iterator().next();
        //EdgeCursor rowCursor=(List)dataCursor.getOrdinateEdge().elementAt(0);
        EdgeCursor columnCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(0);
        EdgeCursor rowCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(1);
        DimensionCursor measureCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(0);
        DimensionCursor channelCursor = (DimensionCursor) pageCursor.getDimensionCursor().get(1);
        DimensionCursor productCursor = (DimensionCursor) rowCursor.getDimensionCursor().get(0);
        DimensionCursor geographyCursor =(DimensionCursor) columnCursor.getDimensionCursor().get(0);
        DimensionCursor timeCursor = (DimensionCursor) columnCursor.getDimensionCursor().get(1);
        columnCursor.setFetchSize(6);
        rowCursor.setFetchSize(4);
        return dataCursor;
    }

    private void print(DimensionView dimensionView, PrintWriter pw) throws OLAPException {
        Connection connection = getConnection();
        Cube salesCube = getCube(connection, "Sales");
        CubeView query = connection.createCubeView();//salesCube
        EdgeView edgeView = query.createOrdinateEdge();
        edgeView.getDimensionView().add(dimensionView);
        CubeCursor cubeCursor = query.createCursor();
        print(cubeCursor, pw);
    }

    private void print(CubeCursor dataCursor, PrintWriter pw) throws OLAPException {
        EdgeCursor columnCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(0);
        EdgeCursor rowCursor = (EdgeCursor) dataCursor.getOrdinateEdge().get(1);
        pw.println("Rows:");
        print(rowCursor, pw);
        pw.println("Columns:");
        print(columnCursor, pw);
        //Model.instance().foo(query);
        pw.flush();
    }

    private Cube getCube(Connection connection, String s) throws OLAPException {
        for (Iterator cubes = connection.getCubes().iterator(); cubes.hasNext();) {
            Cube cube = (Cube) cubes.next();
            if (cube.getName().equals(s)) {
                return cube;
            }
        }
        return null;
    }

    private void print(EdgeCursor cursor, PrintWriter pw) throws OLAPException {
        while (cursor.next()) {
            final List dimensionCursors = cursor.getDimensionCursor();
            int j = 0;
            for (Iterator iterator = dimensionCursors.iterator(); iterator.hasNext();) {
                if (j++ > 0) {
                    pw.print(", ");
                }
                DimensionCursor dimensionCursor = (DimensionCursor) iterator.next();
                pw.print("(");
                final int columnCount = dimensionCursor.getMetaData().getColumnCount();
                for (int i = 0; i < columnCount; i++) {
                    if (i > 0) {
                        pw.print(", ");
                    }
                    final Object o = dimensionCursor.getObject(i);
                }
                pw.print(")");
            }
            pw.println();
        }
    }

    public void testBar() throws OLAPException {
        foo();

        StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        pw.flush();
        assertEquals("", sw.toString());
    }

    private Dimension getDimension(Cube cube, String s) throws OLAPException {
        for (Iterator iterator = cube.getCubeDimensionAssociation().iterator(); iterator.hasNext();) {
            CubeDimensionAssociation cubeDimension = (CubeDimensionAssociation) iterator.next();
            if (cubeDimension.getDimension().getName().equals(s)) {
                return cubeDimension.getDimension();
            }
        }
        return null;
    }

    public static void main(String[] args) {
        TestRunner.run(JolapTest.class);
    }
}

// End JolapTest.java