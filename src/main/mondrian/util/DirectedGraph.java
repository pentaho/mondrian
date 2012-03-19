/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2008-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.util.*;

/**
 * Directed graph.
 *
 * @author jhyde
 * @since 13 September, 2008
 */
public class DirectedGraph<N, E extends DirectedGraph.Edge<N>> {
    private final List<E> edges = new ArrayList<E>();
    private final Map<N, List<E>> linksFrom =
        new HashMap<N, List<E>>();
    private final Map<N, List<Pair<E, Boolean>>> linksToAndFrom =
        new HashMap<N, List<Pair<E, Boolean>>>();

    /**
     * Creates a directed graph.
     */
    public DirectedGraph() {
        super();
    }

    /**
     * Adds an edge to the graph.
     *
     * @param edge Edge
     */
    public void addEdge(E edge) {
        edges.add(edge);

        // add edge to map of links from nodes
        List<E> list = linksFrom.get(edge.getFrom());
        if (list == null) {
            list = new ArrayList<E>();
            linksFrom.put(edge.getFrom(), list);
        }
        list.add(edge);

        // add edge to map of bidirectional links from nodes
        List<Pair<E, Boolean>> list2 = linksToAndFrom.get(edge.getFrom());
        if (list2 == null) {
            list2 = new ArrayList<Pair<E, Boolean>>();
            linksToAndFrom.put(edge.getFrom(), list2);
        }
        list2.add(Pair.of(edge, Boolean.TRUE));

        // add edge to map of bidirectional links to nodes, reversed
        List<Pair<E, Boolean>> list3 = linksToAndFrom.get(edge.getTo());
        if (list3 == null) {
            list3 = new ArrayList<Pair<E, Boolean>>();
            linksToAndFrom.put(edge.getTo(), list3);
        }
        list3.add(Pair.of(edge, Boolean.FALSE));
    }

    /**
     * Finds all paths between two nodes.
     *
     * @param from Start node
     * @param to End node
     * @return List of paths
     */
    public List<List<E>> findAllPaths(N from, N to) {
        if (from.equals(to)) {
            return Collections.singletonList(Collections.<E>emptyList());
        }
        List<E> path = new ArrayList<E>();
        Set<N> activeNodes = new HashSet<N>();
        final List<List<E>> pathList = new ArrayList<List<E>>();
        findSuccessorPaths(pathList, path, activeNodes, from, to);
        return pathList;
    }

    /**
     * Finds all paths between two nodes, considering backward as well as
     * forward edges.
     *
     * @param from Start node
     * @param to End node
     * @return List of paths
     */
    public List<List<Pair<E, Boolean>>> findAllPathsUndirected(N from, N to) {
        if (from.equals(to)) {
            return Collections.singletonList(
                Collections.<Pair<E, Boolean>>emptyList());
        }
        List<Pair<E, Boolean>> path = new ArrayList<Pair<E, Boolean>>();
        Set<N> activeNodes = new HashSet<N>();
        final List<List<Pair<E, Boolean>>> pathList =
            new ArrayList<List<Pair<E, Boolean>>>();
        findSuccessorPathsUndirected(pathList, path, activeNodes, from, to);
        return pathList;
    }

    /**
     * Finds all paths from one node to another, by considering all edges out of
     * of {@code from} and recursively finding paths from those edges to
     * {@code to}.
     *
     * @param pathList List of solution paths
     * @param path Current prefix path
     * @param activeNodes Set of active nodes, for cycle detection
     * @param from From node
     * @param to To node
     */
    private void findSuccessorPaths(
        List<List<E>> pathList,
        List<E> path,
        Set<N> activeNodes,
        N from,
        N to)
    {
        final List<E> successors = linksFrom.get(from);
        if (successors == null) {
            return;
        }
        if (!activeNodes.add(from)) {
            throw new RuntimeException(
                "Graph contains cycle: " + path);
        }
        for (E edge : successors) {
            path.add(edge);
            if (edge.getTo().equals(to)) {
                // We have a solution
                pathList.add(new ArrayList<E>(path));
            }
            findSuccessorPaths(pathList, path, activeNodes, edge.getTo(), to);
            path.remove(path.size() - 1);
        }
        activeNodes.remove(from);
    }

    /**
     * Finds all paths from one node to another, by considering all edges out of
     * of {@code from} and recursively finding paths from those edges to
     * {@code to}.
     *
     * @param pathList List of solution paths
     * @param path Current prefix path
     * @param activeNodes Set of active nodes, for cycle detection
     * @param from From node
     * @param to To node
     */
    private void findSuccessorPathsUndirected(
        List<List<Pair<E, Boolean>>> pathList,
        List<Pair<E, Boolean>> path,
        Set<N> activeNodes,
        N from,
        N to)
    {
        final List<Pair<E, Boolean>> successors = linksToAndFrom.get(from);
        if (successors == null) {
            return;
        }
        if (!activeNodes.add(from)) {
            return;
        }
        for (Pair<E, Boolean> edge : successors) {
            path.add(edge);
            N edgeTo = edge.right ? edge.left.getTo() : edge.left.getFrom();
            if (edgeTo.equals(to)) {
                // We have a solution
                pathList.add(new ArrayList<Pair<E, Boolean>>(path));
            } else {
                findSuccessorPathsUndirected(
                    pathList, path, activeNodes, edgeTo, to);
            }
            path.remove(path.size() - 1);
        }
        activeNodes.remove(from);
    }

    /**
     * Returns a list of edges in the graph.
     *
     * @return List of edges
     */
    public List<E> edgeList() {
        return edges;
    }

    /**
     * Link between two nodes.
     */
    public interface Edge<E> {
        /**
         * Returns the source node of this link.
         *
         * @return source node
         */
        E getFrom();

        /**
         * Returns the target node of this link.
         *
         * @return target node
         */
        E getTo();
    }
}

// End DirectedGraph.java
