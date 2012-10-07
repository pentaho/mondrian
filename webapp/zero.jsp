<%@ page import="mondrian.olap.*" %>
<%@ page import="java.util.*" %>

<%
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 Pentaho
// All Rights Reserved.
%>

<%!
    private static final String ACTION_PARAM = "action";
    private static final String ACTION_UNLIMIT_MEMBERS = "unlimitMembers";
    private static final String ACTION_DESELECT_MEMBERS = "deselectMembers";
    private static final String ACTION_SELECT_MEMBER = "selectMember";
    private static final String ACTION_SET_MEMBER = "setMember";

    private static final String OLAP_URL =
        "Provider=mondrian; Jdbc=jdbc:mysql://localhost/foodmart; JdbcUser=foodmart; JdbcPassword=foodmart; Catalog=file:/c:/open/mondrian/demo/FoodMart.mondrian.xml";

    private Cube lookup(Cube[] cubes, String cubeName) {
        for (Cube cube : cubes) {
            if (cube.getName().equals(cubeName)) {
                return cube;
            }
        }
        return null;
    }

    private ModelMember lookup(List<ModelMember> members, String memberName) {
        for (ModelMember member : members) {
            if (member.member.getName().equals(memberName)) {
                return member;
            }
        }
        return null;
    }

    private ModelDimension lookup(List<ModelDimension> dimensions, String dimensionName) {
        for (ModelDimension dimension : dimensions) {
            if (dimension.dimension.getName().equals(dimensionName)) {
                return dimension;
            }
        }
        return null;
    }


    static class Pane {
        private final List<ModelDimension> dimensions =
            new ArrayList<ModelDimension>();
    }

    static class ModelDimension {
        private final Model model;
        private final Dimension dimension;
        private Member currentMember;
        private boolean currentMemberSelected;
        private final List<ModelMember> members = new ArrayList<ModelMember>();
        private int maxMemberCount;
        private boolean needToRecomputeMembers = true;
        private boolean hasMoreMembersThanShown;

        public ModelDimension(Model model, Dimension dimension) {
            this.model = model;
            this.dimension = dimension;
            this.maxMemberCount = model.getDefaultMaxMemberCount();
            setParentMember(dimension.getHierarchy().getDefaultMember());
        }

        /**
         * Sets the current embmer of this dimension.
         *
         * <p>After calling this method, you
         * need to call bar(, true) to populate the members. If you are
         * setting several dimensions at once, defer the calls until all have
         * been set.
         *
         * @param member
         */
        private void setParentMember(Member member) {
            this.currentMember = member;
            this.members.clear();
            this.needToRecomputeMembers = true;
        }

        public List<ModelMember> getMembers() {
            return members;
        }

        public List<ModelMember> getNonZeroMembers() {
            final ArrayList<ModelMember> list = new ArrayList<ModelMember>();
            for (ModelMember member : members) {
                if (member.count > 0) {
                    list.add(member);
                }
            }
            // Sort by count descending
            Collections.sort(
                list,
                new Comparator<ModelMember>() {
                    public int compare(ModelMember o1, ModelMember o2) {
                        if (o1.count != o2.count) {
                            return o2.count - o1.count;
                        }
                        return o1.member.getUniqueName().compareTo(
                            o2.member.getUniqueName());
                    }
                }
            );
            if (maxMemberCount >= 0 && list.size() > maxMemberCount) {
                return list.subList(0, maxMemberCount);
            }
            return list;
        }

        public String getCaption() {
            return dimension.getCaption();
        }

        public String urlToRemove() {
            return "zero.jsp?action=" + ACTION_DESELECT_MEMBERS
                + "&dimension=" + dimension.getName();
        }

        public String urlToRemoveLimit() {
            return "zero.jsp?action=" + ACTION_UNLIMIT_MEMBERS
                + "&dimension=" + dimension.getName();
        }

        public boolean existsSelected() {
            if (needToRecomputeMembers) {
                return false;
            }
            if (currentMemberSelected) {
                return true;
            }
            for (ModelMember member : members) {
                if (member.selected) {
                    return true;
                }
            }
            return false;
        }

        public String urlToSetMember(Member member) {
            assert member.getDimension() == dimension;
            return "zero.jsp?action=" + ACTION_SET_MEMBER
                + "&dimension="
                + dimension.getName()
                + "&member=" + member.getUniqueName();
        }
    }

    static class ModelMember {
        final Member member;
        boolean selected;
        private int count;

        ModelMember(Member member) {
            System.out.println("create member " + member);
            this.member = member;
        }

        public String urlToAdd() {
            return "zero.jsp?action=" + ACTION_SELECT_MEMBER
                + "&dimension="
                + member.getDimension().getName()
                + "&member=" + member.getName();
        }

        public String getCaption() {
            return member.getCaption();
        }

        public int getCount() {
            return count;
        }
    }

    static class Model {
        private final Connection connection;
        private final Cube cube;
        private List<ModelDimension> dimensions =
            new ArrayList<ModelDimension>();

        public Model(Connection connection, Cube cube) {
            this.connection = connection;
            this.cube = cube;

            for (Dimension dimension : cube.getDimensions()) {
                if (!dimension.isMeasures()) {
                    dimensions.add(new ModelDimension(this, dimension));
                }
            }
            foo(null);
        }

        /**
         * After the selection of a given dimension has changed, recomputes the
         * cardinality of the members in all other dimensions.
         *
         * @param modelDimension Dimension which changed, or null to update
         * everything
         */
        void foo(ModelDimension modelDimension) {
            // Are there any dimensions which need recomputing?
            boolean exist = true; // todo:
            for (ModelDimension dimension : dimensions) {
                if (dimension.needToRecomputeMembers) {
                    exist = true;
                }
            }
            // If any dimensions need recomputing, all dimensions with no
            // selected members float up to their 'all' members. They will
            // float down again.
            if (exist) {
                for (ModelDimension dimension : dimensions) {
                    if (!dimension.needToRecomputeMembers
                        && !dimension.existsSelected()) {
                        dimension.setParentMember(
                            dimension.dimension.getHierarchy().getDefaultMember());
                    }
                }
            }
            for (ModelDimension dimension : dimensions) {
                bar(dimension);
            }
        }

        /**
         * Recomputes the cardinality of the members of a given dimension.
         *
         * <p>If the dimension has only one member with non-zero cardinality,
         * that member becomes the current member.
         *
         * <p>If the dimension has no non-zero members, the dimension is
         * flagged invisible. (TODO:)
         *
         * @param modelDimension Dimension whose cardinality compute
         */
        void bar(ModelDimension modelDimension) {
            String axis;
            if (modelDimension.needToRecomputeMembers) {
                axis =
                    "{"
                        + modelDimension.currentMember.getUniqueName()
                        + ".Children}";
                if (modelDimension.maxMemberCount >= 0) {
                    axis =
                        "TopCount(" + axis
                            + ", " + (modelDimension.maxMemberCount + 1) + ")";
                }
            } else {
                StringBuilder buf = new StringBuilder("{");
                int k = -1;
                for (ModelMember modelMember : modelDimension.getMembers()) {
                    if (++k > 0) {
                        buf.append(",\n");
                    }
                    buf.append(modelMember.member.getUniqueName());
                }
                buf.append("}");
                axis = buf.toString();
            }
            StringBuilder withSet = new StringBuilder();
            StringBuilder slicer = new StringBuilder();
            for (ModelDimension dimension : dimensions) {
                if (dimension == modelDimension) {
                    continue;
                }
                String theMember;
                if (dimension.existsSelected()) {
                    if (withSet.length() == 0) {
                        withSet.append("WITH ");
                    }
                    theMember = dimension.dimension.getUniqueName() + ".[Foo]";
                    withSet.append("MEMBER ")
                        .append(theMember)
                        .append(" AS 'Aggregate({");
                    int k = -1;
                    for (ModelMember modelMember : dimension.getMembers()) {
                        if (!modelMember.selected) {
                            continue;
                        }
                        ++k;
                        if (k > 0) {
                            withSet.append(", ");
                        }
                        withSet.append(modelMember.member.getUniqueName());
                    }
                    withSet.append("})'\n");
                } else {
                    theMember = dimension.currentMember.getUniqueName();
                }
                if (slicer.length() == 0) {
                    slicer.append("\nWHERE (");
                } else {
                    slicer.append(", ");
                }
                slicer.append(theMember);
            }
            if (slicer.length() > 0) {
                slicer.append(")");
            }
            String mdx =
                withSet
                    + "SELECT NON EMPTY " + axis + " ON COLUMNS\n"
                    + "FROM " + cube.getUniqueName()
                    + slicer;
            System.out.println("MDX:\n    " + mdx);
            Query query = connection.parseQuery(mdx);
            Result result = connection.execute(query);
            int nonZeroMembers = 0;
            if (modelDimension.needToRecomputeMembers) {
                modelDimension.members.clear();
                int i = -1;
                modelDimension.hasMoreMembersThanShown = false;
                for (Position position : result.getAxes()[0].getPositions()) {
                    ++i;
                    if (modelDimension.maxMemberCount >= 0
                        && i > modelDimension.maxMemberCount - 1) {
                        modelDimension.hasMoreMembersThanShown = true;
                    }
                    final Cell cell = result.getCell(new int[]{i});
                    final int count = ((Number) cell.getValue()).intValue();
                    if (count > 0) {
                        ++nonZeroMembers;
                    }
                    ModelMember modelMember = new ModelMember(position.get(0));
                    modelMember.count = count;
                    modelDimension.members.add(modelMember);
                }
                modelDimension.needToRecomputeMembers = false;
            } else {
                int i = -1;
                Map<Member, Integer> memberCounts = new HashMap<Member, Integer>();
                for (Position position : result.getAxes()[0].getPositions()) {
                    ++i;
                    final Cell cell = result.getCell(new int[]{i});
                    memberCounts.put(
                        position.get(0),
                        ((Number) cell.getValue()).intValue());
                }
                for (ModelMember modelMember : modelDimension.getMembers()) {
                    final Integer integer = memberCounts.get(modelMember.member);
                    final int count;
                    if (integer == null) {
                        count = 0;
                    } else {
                        count = integer;
                        assert count > 0;
                        ++nonZeroMembers;
                    }
                    modelMember.count = count;
                }
            }
            if (nonZeroMembers == 1
                && modelDimension.currentMember.getLevel().getDepth() <
                modelDimension.currentMember.getHierarchy().getLevels().length - 1) {
                // Drill down, making the sole remaining member the parent.
                for (ModelMember modelMember : modelDimension.getMembers()) {
                    if (modelMember.count > 0) {
                        modelDimension.setParentMember(modelMember.member);
                        // recursive call
                        bar(modelDimension);
                        return;
                    }
                }
            }
        }

        /**
         * Returns the maximum number of members to display in a dimension.
         * Dimensions are created with this setting, and reset to this on
         * drillup or drilldown.
         * Return -1 if you want to always show all members.
         *
         * @return maximum number of members to display in a dimension, or -1
         * to always show all members
         */
        public int getDefaultMaxMemberCount() {
            return 7;
        }
    }
%>

<%
    Model model;
    String cubeName = request.getParameter("cubeName");
    if (cubeName != null) {
        model = null;
    } else {
        model = (Model) session.getAttribute("ctx");
    }
    if (model == null) {
        if (cubeName == null) {
            cubeName = "Sales";
        }
        Connection connection = DriverManager.getConnection(OLAP_URL, null);
        Cube cube = lookup(connection.getSchema().getCubes(), cubeName);
        if (cube == null) {
%> Cube <%= cubeName %> not found.<%
            return;
        }
        model = new Model(connection, cube);
        session.setAttribute("ctx", model);
    }

    final String action = request.getParameter(ACTION_PARAM);
    if (action == null) {
        // nothing
    } else if (action.equals(ACTION_SELECT_MEMBER)) {
        String dimensionName = request.getParameter("dimension");
        String memberName = request.getParameter("member");
        ModelDimension modelDimension = lookup(model.dimensions, dimensionName);
        if (modelDimension == null) {
            throw new IllegalArgumentException();
        }
        ModelMember modelMember = lookup(modelDimension.getMembers(), memberName);
        if (modelMember == null) {
            throw new IllegalArgumentException();
        }
        System.out.println("select " + modelMember.member);
        modelDimension.currentMemberSelected = false;
        modelMember.selected = true;
        model.foo(modelDimension);
    } else if (action.equals(ACTION_DESELECT_MEMBERS)) {
        String dimensionName = request.getParameter("dimension");
        ModelDimension modelDimension = lookup(model.dimensions, dimensionName);
        if (modelDimension == null) {
            throw new IllegalArgumentException();
        }
        for (ModelMember modelMember : modelDimension.getMembers()) {
            modelMember.selected = false;
        }
        modelDimension.currentMemberSelected = false;
        modelDimension.maxMemberCount = model.getDefaultMaxMemberCount();
        model.foo(modelDimension);
    } else if (action.equals(ACTION_UNLIMIT_MEMBERS)) {
        String dimensionName = request.getParameter("dimension");
        ModelDimension modelDimension = lookup(model.dimensions, dimensionName);
        if (modelDimension == null) {
            throw new IllegalArgumentException();
        }
        modelDimension.hasMoreMembersThanShown = false;
        modelDimension.maxMemberCount = -1;
    } else if (action.equals(ACTION_SET_MEMBER)) {
        String dimensionName = request.getParameter("dimension");
        ModelDimension modelDimension = lookup(model.dimensions, dimensionName);
        if (modelDimension == null) {
            throw new IllegalArgumentException();
        }
        String memberUniqueName = request.getParameter("member");
        final Member member = (Member)
            model.cube.getSchema().getSchemaReader().lookupCompound(
                model.cube,
                Util.parseIdentifier(memberUniqueName),
                true,
                Category.Member);
        modelDimension.setParentMember(member);
        modelDimension.currentMemberSelected = true;
        model.foo(modelDimension);
    } else {
        throw new RuntimeException("Unknown action '" + action + "'");
    }

    // Assign dimensions to panes
    List<Pane> paneList = new ArrayList<Pane>();
    int paneCount = 3;
    for (int i = 0; i < paneCount; ++i) {
        Pane pane = new Pane();
        paneList.add(pane);
    }
    int i = -1;
    for (ModelDimension modelDimension : model.dimensions) {
        ++i;
        int paneIndex = i % paneCount;
        paneList.get(paneIndex).dimensions.add(modelDimension);
    }
%>
<html>
<head>
</head>
<body>
<table border="1" width="100%">
<tr>
  <table>
    <tr>
        <%
            for (Pane pane : paneList) {
        %>
        <td valign="top" width="240">
            <%
                for (ModelDimension dimension : pane.dimensions) {
            %>
            <p><font size="2"><b><%= dimension.getCaption() %></b>
                <%
                    Member m = dimension.currentMember;
                    if (m != null && !m.isAll()) {
                        List<Member> ancestors = new LinkedList<Member>();
                        while (m != null && !m.isAll()) {
                            ancestors.add(0, m);
                            m = m.getParentMember();
                        }
                        for (Member ancestor : ancestors) {
                            if (dimension.currentMemberSelected &&
                                ancestor == dimension.currentMember) {
                %>
                <a href="<%= dimension.urlToSetMember(ancestor) %>"><b><%= ancestor.getCaption() %></b></a>
                <%
                            } else {
                %>
                <a href="<%= dimension.urlToSetMember(ancestor) %>"><%= ancestor.getCaption() %></a>
                <%
                            }
                        }
                    }
                %>
                [<a href="<%= dimension.urlToRemove() %>">x</a>]<%
                    if (dimension.currentMember.getLevel().getChildLevel() != null) {
                %>:<%
                    }
                %>
            </font></p>
            <%
                if (dimension.currentMember.getLevel().getChildLevel() != null) {
          %>
            <blockquote>
                <%
                    int k = -1;
                    for (ModelMember member : dimension.getNonZeroMembers()) {
                        ++k;
                        if (k > 0) {
                %>, <%
                }
                if (member.selected) {
            %>
                <font size="2"><b><%= member.getCaption() %></b></font><font size="2"> (<%= member.getCount() %>)
                <%
                            } else {
                %>
                <a href="<%= member.urlToAdd() %>"><font size="2"><%= member.getCaption() %></font></a><font size="2"> (<%= member.getCount() %>)<%
                            }
                        }
                        if (dimension.hasMoreMembersThanShown) {
                            if (k > 0) {
                    %>, <%
                            }
                            %><a href="<%= dimension.urlToRemoveLimit() %>">...</a><%
                        }
                      %>
            </blockquote>
            <%
                }
            %>
            <%
                }
            %>
        </td>
        <%
            }
        %>
    </tr>
</table>
</body>
</html>