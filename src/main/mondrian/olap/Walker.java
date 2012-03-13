/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
//
// jhyde, 1 March, 1999
*/
package mondrian.olap;

import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Stack;

/**
 * Walks over a tree, returning nodes in prefix order.  Objects which are an
 * instance of {@link Walkable} supply their children using
 * <code>getChildren()</code>; other objects are assumed to have no children.
 *
 * <p>If the tree is modified during the enumeration, strange things may happen.
 *
 * <p>Example use:<code><pre>
 *    Tree t;
 *    Walker w = new Walker(t);
 *    while (w.hasMoreElements()) {
 *      Tree node = (Tree) w.nextNode();
 *      System.out.println(node.toString());
 *    }
 * </pre></code>
 */
public class Walker implements Enumeration {
    /**
     * The active parts of the tree from the root to nextNode are held in a
     * stack.  When the stack is empty, the enumeration finishes.  currentFrame
     * holds the frame of the 'current node' (the node last returned from
     * nextElement()) because it may no longer be on the stack.
     */
    private final Stack stack;
    private Frame currentFrame;
    private Object nextNode;

    private class Frame {
        Frame(Frame parent, Object node) {
            this.parent = parent;
            this.node = node;
            this.children = getChildren(node);
            this.childIndex = -1; // haven't visited first child yet
        }
        final Frame parent;
        final Object node;
        final Object[] children;
        int childIndex;
    }

    public Walker(Walkable root)
    {
        stack = new Stack();
        currentFrame = null;
        visit(null, root);
    }

    private void moveToNext()
    {
        if (stack.empty()) {
            return;
        }
        currentFrame = (Frame) stack.peek();

        // Unwind stack until we find a level we have not completed.
        do {
            Frame frame = (Frame) stack.peek();
            if (frame.children != null
                && ++frame.childIndex < frame.children.length)
            {
                // Here is an unvisited child.  Visit it.
                visit(frame, frame.children[frame.childIndex]);
                return;
            }
            stack.pop();
        } while (!stack.empty());
        nextNode = null;
    }

    private void visit(Frame parent, Object node)
    {
        nextNode = node;
        stack.addElement(new Frame(parent, node));
    }

    public boolean hasMoreElements()
    {
        return nextNode != null;
    }

    public Object nextElement()
    {
        moveToNext();
        return currentFrame.node;
    }

    /** Tell walker that we don't want to visit any (more) children of this
     * node.  The next node visited will be (a return visit to) the node's
     * parent.  Not valid until nextElement() has been called. */
    public void prune()
    {
        if (currentFrame.children != null) {
            currentFrame.childIndex = currentFrame.children.length;
        }
        //we need to make that next frame on the stack is not a child
        //of frame we just pruned. if it is, we need to prune it too
        if (this.hasMoreElements()) {
            Object nextFrameParentNode = ((Frame)stack.peek()).parent.node;
            if (nextFrameParentNode != currentFrame.node) {
                return;
            }
            //delete the child of current member from the stack
            stack.pop();
            if (currentFrame.parent != null) {
                currentFrame = currentFrame.parent;
            }
            nextElement();
        }
    }

    public void pruneSiblings()
    {
        prune();
        currentFrame = currentFrame.parent;
        if (currentFrame != null) {
            prune();
        }
    }


    /** returns the current object.  Not valid until nextElement() has been
        called. */
    public Object currentElement()
    {
        return currentFrame.node;
    }

    /** returns level in the tree of the current element (that is, last element
     * returned from nextElement()).  The level of the root element is 0. */
    public int level()
    {
        int i = 0;
        for (Frame f = currentFrame; f != null; f = f.parent) {
            i++;
        }
        return i;
    }

    public final Object getParent()
    {
        return getAncestor(1);
    }

    public final Object getAncestor(int iDepth)
    {
        Frame f = getAncestorFrame(iDepth);
        return f == null ? null : f.node;
    }

    /** returns the <code>iDepth</code>th ancestor of the current element */
    private Frame getAncestorFrame(int iDepth)
    {
        for (Frame f = currentFrame; f != null; f = f.parent) {
            if (iDepth-- == 0) {
                return f;
            }
        }
        return null;
    }

    /** get the ordinal within its parent node of the current node.  Returns 0
        for the root element.  Equivalent to getAncestorOrdinal(0). */
    public int getOrdinal()
    {
        // We can't use currentFrame.parent.iChild because moveToNext() may
        // have changed it.
        return currentFrame.parent == null
            ? 0
            : arrayFind(currentFrame.parent.children, currentFrame.node);
    }

    /** get the ordinal within its parent node of the <code>iDepth</code>th
     * ancestor. */
    public int getAncestorOrdinal(int iDepth)
    {
        Frame f = getAncestorFrame(iDepth);
        return f == null
            ? -1
            : f.parent == null
            ? 0
            : arrayFind(f.parent.children, f.node);
    }

    /** Override this function to prune the tree, or to allow objects which are
     * not Walkable to have children. */
    public Object[] getChildren(Object node)
    {
        if (node instanceof Walkable) {
            return ((Walkable) node).getChildren();
        } else {
            return null;
        }
    }

    private static int arrayFind(Object[] array, Object o)
    {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == o) {
                return i;
            }
        }
        return -1;
    }

    private static class Region implements Walkable
    {
        String name;
        Region[] children;

        Region(String name, Region[] children)
        {
            this.name = name;
            this.children = children;
        }

        public Object[] getChildren() {
            return children;
        }

        public static void walkUntil(Walker walker, String name) {
            while (walker.hasMoreElements()) {
                Region region = (Region) walker.nextElement();
                if (region.name.equals(name)) {
                    break;
                }
            }
        }
    };

    public static void main(String[] args)
    {
        PrintWriter pw = new PrintWriter(System.out);
        Region usa = new Region(
            "USA", new Region[] {
            new Region(
                "CA", new Region[] {
                    new Region(
                        "San Francisco", new Region[] {
            new Region(
                "WesternAddition", new Region[] {
                    new Region("Haight", null)}),
                    new Region("Soma", null)
                }),
                new Region("Los Angeles", null)}),
            new Region(
                "WA", new Region[] {
                    new Region("Seattle", null),
                    new Region("Tacoma", null)})});

        Walker walker = new Walker(usa);
        if (false) {
            while (walker.hasMoreElements()) {
                Region region = (Region) walker.nextElement();
                pw.println(region.name);
                pw.flush();
            }
        }

        Region.walkUntil(walker, "CA");
        walker.prune();
        Region region = (Region) walker.nextElement(); // should be WA
        pw.println(region.name);
        pw.flush();

        walker = new Walker(usa);
        Region.walkUntil(walker, "USA");
        walker.prune();
        region = (Region) walker.nextElement(); // should be null
        if (region == null) {
            pw.println("null");
        }
        pw.flush();

        walker = new Walker(usa);
        Region.walkUntil(walker, "Los Angeles");
        walker.prune();
        region = (Region) walker.nextElement(); // should be WA
        pw.println(region.name);
        pw.flush();

        walker = new Walker(usa);
        Region.walkUntil(walker, "Haight");
        walker.prune();
        region = (Region) walker.nextElement(); // should be Soma
        pw.println(region.name);
        pw.flush();

        walker = new Walker(usa);
        Region.walkUntil(walker, "Soma");
        walker.prune();
        region = (Region) walker.nextElement(); // should be Los Angeles
        pw.println(region.name);
        pw.flush();

        walker = new Walker(usa);
        Region.walkUntil(walker, "CA");
        walker.pruneSiblings();
        region = (Region) walker.nextElement(); // should be Los Angeles
        if (region == null) {
            pw.println("null");
            pw.flush();
        }

        walker = new Walker(usa);
        Region.walkUntil(walker, "Soma");
        walker.pruneSiblings();
        region = (Region) walker.nextElement(); // should be Los Angeles
        if (region == null) {
            pw.println("null");
            pw.flush();
        }
    }
}


// End Walker.java
