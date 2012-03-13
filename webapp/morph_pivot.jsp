<%--
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde and others
// All Rights Reserved.
//
// Julian Hyde, June 20, 2002
--%>
<%@ page language="java"
         import="mondrian.web.taglib.ResultCache,
                 mondrian.olap.*,
                 java.io.PrintWriter,
                 java.util.ArrayList,
                 java.util.List,
                 java.util.Iterator,
                 java.util.LinkedList,
                 java.io.IOException"%>
<%@ taglib uri="/WEB-INF/mdxtable.tld" prefix="mdx" %>
<%!
    static final String queryName = "pivotQuery";
    static final String redirect = "pivot.jsp";

    String actionURL(String operation) {
        return "mdxquery?query=" + queryName +
            "&" + operation +
            "&redirect=" + redirect;
    }
    String removeHierarchyUrl(Hierarchy hierarchy) {
        return actionURL(
                "operation=remove_hierarchy&hierarchy=" +
                hierarchy.getUniqueName());
    }
    String moveHierarchyUpURL(Hierarchy hierarchy) {
        return actionURL(
                "operation=move_hierarchy_up&hierarchy=" +
                hierarchy.getUniqueName());
    }
    String moveHierarchyDownURL(Hierarchy hierarchy) {
        return actionURL(
                "operation=move_hierarchy_down&hierarchy=" +
                hierarchy.getUniqueName());
    }
    String moveHierarchyToAxisURL(Hierarchy hierarchy, int targetAxis) {
        return actionURL(
                "operation=move_hierarchy_to_axis&hierarchy=" +
                hierarchy.getUniqueName() + "&to_axis=" + targetAxis);
    }
    String replaceHierarchyURL(Hierarchy hierarchy, String expression) {
        return actionURL(
                "operation=replace_hierarchy&hierarchy=" +
                hierarchy.getUniqueName() + "&expression=" + expression);
    }
    String setSlicerURL(Member member) {
        return actionURL(
                "operation=set_slicer&member=" + member.getUniqueName());
    }
    String doubleQuote(String s) {
        return Util.quoteForMdx(s);
    }
    String makeHref(String url, String text) {
        if (url == null) {
            return "<i>" + text + "</i>";
        } else {
            return "<a href=" + doubleQuote(url) + ">" + text + "</a>";
        }
    }
    ArrayList getMembersOnAxis(Result result, int axisOffset, int offset) {
        ArrayList list = new ArrayList();
        Position[] positions = getAxis(result,axisOffset).positions;
        for (int i = 0; i < positions.length; i++) {
            Position position = positions[i];
            if (!list.contains(position.members[offset])) {
                list.add(position.members[offset]);
            }
        }
        return list;
    }
    /**
     * Returns all of a member's ancestors, including itself.
     */
    Member[] getAncestors(Member member) {
        LinkedList list = new LinkedList();
        for (Member m = member; m != null; m = m.getParentMember()) {
            list.addFirst(m);
        }
        return (Member[]) list.toArray(new Member[list.size()]);
    }
    /**
     * Returns all of a member's ancestors, including itself, and their
     * siblings, and its children, in prefix order.
     */
    Member[] getAncestorsAndSiblings(Member member) {
        LinkedList list = new LinkedList();
        Member[] children = getSchemaReader(member).getMemberChildren(member);
        if (children != null && children.length > 0) {
            member = children[0];
        }
        getAncestorsAndSiblings(member,list);
        return (Member[]) list.toArray(new Member[list.size()]);
    }
    void getAncestorsAndSiblings(Member member, List list) {
        // member's parent
        //   older siblings
        //   member
        //     member's children
        //   younger siblings
        Member parent = member.getParentMember();
        Member[] siblings;
        int parentPos;
        if (parent != null) {
            getAncestorsAndSiblings(parent, list);
            parentPos = list.indexOf(parent);
            siblings = getSchemaReader(member).getMemberChildren(parent);
        } else {
            parentPos = -1;
            final Hierarchy hierarchy = member.getHierarchy();
            final Schema schema = hierarchy.getDimension().getSchema();
            siblings = schema.getSchemaReader().getHierarchyRootMembers(hierarchy);
        }
        for (int i = 0; i < siblings.length; i++) {
            Member sibling = siblings[i];
            list.add(parentPos + 1 + i, sibling);
        }
    }
    void generateAxis(Result result, int axis, JspWriter out)
            throws IOException {
        Hierarchy[] hierarchies = getHierarchiesOnAxis(result, axis);
        out.write("<h2>" + getAxisName(axis) + "</h2>"); // e.g. "<p>Columns:</p>"
        out.write("<ul>");
        for (int i = 0; i < hierarchies.length; i++) {
            Hierarchy hierarchy = hierarchies[i];
            out.write("<li>");
            out.write(hierarchy.getName());
            out.write(" [" + makeHref(removeHierarchyUrl(hierarchy), "remove") + "]");
            out.write(" [" + makeHref(i == 0 ? null : moveHierarchyUpURL(hierarchy), "move up") + "]");
            out.write(" [" + makeHref(i == hierarchies.length - 1 ? null : moveHierarchyDownURL(hierarchy), "move down") + "]");
            for (int j = -1; j < result.getAxes().length; j++) {
                if (j != axis) {
                    out.write(" [" + makeHref(moveHierarchyToAxisURL(hierarchy,j), "move to " + getAxisName(j)) + "]");
                }
            }
            if (axis == SLICER_AXIS) {
                // Want to generate something like the following.

//          <li>Time [remove] [move to columns] [move to rows]<blockquote>
//          <table border="1" cellspacing="0" id="AutoNumber12">
///           <tr>
//              <td width="100%"><u>1997</u><br>
//    &nbsp; <u>Q1</u><br>
//    &nbsp; <u>Q2</u><br>
//    &nbsp; Q3 (current)<br>
//    &nbsp;&nbsp;&nbsp; <u>July</u><br>
//    &nbsp;&nbsp;&nbsp; <u>August</u><br>
//    &nbsp;&nbsp;&nbsp; <u>September</u><br>
//    &nbsp; <u>Q4</u><br>
//              <u>1998</u><br>
//              <u>1999</u></td>
//            </tr>
//          </table>
//          </blockquote></li>

                out.write("<blockquote><table border=1 cellspacing=0><tr><td>");
                Member member = result.getSlicerAxis().positions[0].members[i];
                Member[] ancestors = getAncestorsAndSiblings(member);
                for (int j = 0; j < ancestors.length; j++) {
                    if (j > 0) {
                        out.write("<br/>");
                        out.newLine();
                    }
                    Member ancestor = ancestors[j];
                    for (int k = 0, depth = ancestor.getLevel().getDepth(); k < depth; k++) {
                        out.write("&nbsp;&nbsp;&nbsp;");
                    }
                    if (ancestor == member) {
                        out.write(ancestor.getName() + " (current)");
                    } else {
                        out.write(makeHref(setSlicerURL(ancestor), ancestor.getName()));
                    }
                }
                out.write("</td></tr></table></blockquote>");
            }

            out.write("</li>");
            out.newLine();
            if (hierarchy.getDimension().isMeasures()) {
                out.write("<ul>");
                List membersList = getMembersOnAxis(result, axis, i);
                for (Iterator iterator = membersList.iterator(); iterator.hasNext();) {
                    Member member = (Member) iterator.next();
                    out.write("<li>" + member.getName());
                    out.write(" [" + makeHref(replaceHierarchyURL(hierarchy,"{todo: [Measures].[A],[Measures].[B], [Measures].[C]}"),"move up") + "]");
                    out.write(" [" + makeHref(replaceHierarchyURL(hierarchy,"{todo: [Measures].[A],[Measures].[B], [Measures].[C]}"),"move down") + "]");
                    out.write(" [" + makeHref(replaceHierarchyURL(hierarchy,"{todo: [Measures].[A],[Measures].[B], [Measures].[C]}"),"remove") + "]");
                    out.write("</li>");
                    out.newLine();
                }
                final SchemaReader schemaReader = hierarchy.getDimension().getSchema().getSchemaReader();
                Member[] allMeasures = schemaReader.getHierarchyRootMembers(hierarchy);
                for (int j = 0; j < allMeasures.length; j++) {
                    Member measure = allMeasures[j];
                    if (membersList.contains(measure)) {
                        continue;
                    }
                    out.write("<li>" + measure.getName());
                    out.write(" [" + makeHref(replaceHierarchyURL(hierarchy,"{todo: [Measures].[A],[Measures].[B], [Measures].[C]}"),"add") + "]");
                    out.write("</li>");
                    out.newLine();
                }
                out.write("</ul>");
                out.newLine();
            }
        }
        out.write("</ul>");
        out.newLine();
    }
    SchemaReader getSchemaReader(OlapElement element) {
        return null;
    }
    Hierarchy[] getHierarchiesOnAxis(Result result, int axis) {
        Axis resultAxis = getAxis(result,axis);
        Position position = resultAxis.positions[0];
        int count = position.members.length;
        Hierarchy[] hierarchies = new Hierarchy[count];
        for (int i = 0; i < hierarchies.length; i++) {
            hierarchies[i] = position.members[i].getHierarchy();
        }
        return hierarchies;
    }
    boolean isHierarchyUsed(Result result, Hierarchy hierarchy) {
        for (int i = -1, axisCount = result.getAxes().length;
                i < axisCount; i++) {
            Axis axis = getAxis(result, i);
            for (int j = 0; j < axis.positions[0].members.length; j++) {
                Member member = axis.positions[0].members[j];
                if (member.getHierarchy() == hierarchy) {
                    return true;
                }
            }
        }
        return false;
    }
    Axis getAxis(Result result, int axis) {
        if (axis < 0) {
            return result.getSlicerAxis();
        } else {
            return result.getAxes()[axis];
        }
    }
    private static final int SLICER_AXIS = -1;
    private static final int COLUMNS_AXIS = 0;
    private static final int ROWS_AXIS = 1;
    String getAxisName(int axis) {
        switch (axis) {
        case -1: return "slicer";
        case 0: return "columns";
        case 1: return "rows";
        default: return "axis#" + axis;
        }
    }
%>

<html>
<head>
  <title>Morph Pivot Table</title>
</head>

<%
    ResultCache rc = ResultCache.getInstance(
            pageContext.getSession(), pageContext.getServletContext(), queryName);
    Query query = rc.getQuery();
    QueryAxis[] axes = query.axes;
    Result result = rc.getResult();
%>

<body>
<h1>Organize hierarchies</h1>
<%
    generateAxis(result, COLUMNS_AXIS, out);
    generateAxis(result, ROWS_AXIS, out);
    generateAxis(result, SLICER_AXIS, out);
%>

<h2>Hierarchies not shown:</h2>
<ul>
<%
    Cube cube = query.getCube();
    for (int i = 0; i < cube.getDimensions().length; i++) {
        Dimension dimension = cube.getDimensions()[i];
        for (int j = 0; j < dimension.getHierarchies().length; j++) {
            Hierarchy hierarchy = dimension.getHierarchies()[j];
            if (isHierarchyUsed(result, hierarchy)) {
                continue;
            }
            out.write("<li>" + hierarchy.getName());
            for (int k = -1; k < result.getAxes().length; k++) {
                out.write(" [" + makeHref(moveHierarchyToAxisURL(hierarchy,k), "add to " + getAxisName(k)) + "]");
            }
            out.write("</li>");
            out.newLine();
        }
    }
%>
</ul>

<p>[<u>OK</u>]</td>

</body>
<%-- End morph_pivot.jsp --%>