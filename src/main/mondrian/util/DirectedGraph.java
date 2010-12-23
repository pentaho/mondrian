/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2008 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import java.util.*;

/**
 * Directed graph.
 *
 * @author jhyde
 * @since 13 September, 2008
 * @version $Id$
*/
public class DirectedGraph<N, E extends DirectedGraph.Edge<N>> {
    private final List<E> edges = new ArrayList<E>();
    private final Map<N, List<E>> linksFrom =
        new HashMap<N, List<E>>();

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
        List<E> list = linksFrom.get(edge.getFrom());
        if (list == null) {
            list = new ArrayList<E>();
            linksFrom.put(edge.getFrom(), list);
        }
        list.add(edge);
    }

    /**
     * Finds all paths between two nodes.
     *
     * @param from Start node
     * @param to End node
     * @return List of paths
     */
    public List<List<E>> findAllPaths(N from, N to) {
        final List<List<E>> pathList = new ArrayList<List<E>>();
        if (from.equals(to)) {
            pathList.add(Collections.<E>emptyList());
        }
        List<E> path = new ArrayList<E>();
        Set<N> activeNodes = new HashSet<N>();
        findSuccessorPaths(pathList, path, activeNodes, from, to);
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
