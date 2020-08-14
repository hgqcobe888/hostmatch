package com.spd.util;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

public class MatcherUtil implements Serializable {

    class MatchContext implements Serializable {
        Set<Integer> TREE_SET = new TreeSet<Integer>();
        List<MatchResult> MATCH_RESULT = new ArrayList<MatchResult>();
        String path;
    }

    public Map<Character, Node> TREE_MAP = new TreeMap<Character, Node>();
    private MatchContext matchContext = new MatchContext();

    public static class MatchResult implements Serializable {
        public String pattern;
        public Object data;
        static MatchResult[] EMPTY = new MatchResult[0];

        public MatchResult(String pattern, Object data) {
            this.pattern = pattern;
            this.data = data;
        }
    }

    class Node implements Serializable {
        private String pattern;
        private Node parent;
        private char _char;
        private Map<Character, Node> nodes = null;
        private List<Object> data = null;

        public Node(String pattern, char _char, Node parent) {
            this.pattern = pattern;
            this._char = _char;
            this.parent = parent;
        }

        void addPattern(String pattern, int idx, Object data) {
            if (idx + 1 < pattern.length()) {
                if (nodes == null)
                    nodes = new TreeMap<Character, Node>();
                Node n;
                char c = pattern.charAt(idx + 1);
                if (nodes.containsKey(c))
                    n = nodes.get(c);
                else {
                    n = new Node(pattern, c, this);
                    nodes.put(c, n);
                }
                n.addPattern(pattern, idx + 1, data);
            } else if (pattern.length() == idx + 1) {
                if (this.data == null)
                    this.data = new LinkedList<Object>();
                this.data.add(data);
                this.pattern = pattern;
            } else {
                throw new IllegalStateException(String.format("error index %s of pattern : %s", idx, pattern));
            }
        }

        void buildString(StringBuffer sb, String prefix) {
            if (data != null) {
                sb.append(prefix);
                sb.append(_char);
                sb.append(" > ");
                sb.append(StringUtils.join(data, ','));
                sb.append('\n');
            }
            prefix += _char;
            if (nodes != null)
                for (Node n : nodes.values()) {
                    n.buildString(sb, prefix);
                }
        }

        void find(char[] chars, int idx) {
            if (idx < chars.length) {
                if (nodes != null) {
                    char c = chars[idx];

                    if (nodes.containsKey(c)) {
                        Node n = nodes.get(c);
                        n.find(chars, idx + 1);
                    }

                    if (nodes.containsKey('*')) {
                        Node anyNode = nodes.get('*');
                        for (int index = idx; index <= chars.length; index++) {
                            anyNode.find(chars, index);
                        }
                    }
                }
            } else {
                if (nodes != null)
                    if (nodes.containsKey('*')) {
                        Node anyNode = nodes.get('*');
                        anyNode.find(chars, chars.length);
                    }
                if (data != null) {
                    for (int i = 0; i < data.size(); i++) {
                        matchContext.MATCH_RESULT.add(new MatchResult(pattern, data.get(i)));
                    }
                }
            }
        }
        private String getRoute() {
            return parent == null ? String.valueOf(_char) : (parent.getRoute() + _char);
        }
    }


    public void addPattern(String pattern, Object data) {
        if (pattern.length() == 0)
            throw new IllegalArgumentException("empty pattern");

        char c = pattern.charAt(0);
        Node n;
        if (TREE_MAP.containsKey(c)) {
            n = TREE_MAP.get(c);
        } else {
            n = new Node(pattern, c, null);
            TREE_MAP.put(c, n);
        }
        n.addPattern(pattern, 0, data);
    }

    @Override
    public String toString() {
        StringBuffer bf = new StringBuffer();
        for (Node n : TREE_MAP.values()) {
            n.buildString(bf, "");
        }
        return bf.toString();
    }

    public MatchResult[] matchs(String path) {
        if (null == path || "".equals(path))
            return MatchResult.EMPTY;

        matchContext.TREE_SET.clear();
        matchContext.MATCH_RESULT.clear();
        matchContext.path = path;
        char[] chars = path.toCharArray();

        char c = chars[0];
        if (TREE_MAP.containsKey(c))
            System.out.println(TREE_MAP.get(c).nodes);
            TREE_MAP.get(c).find(chars, 1);

        if (TREE_MAP.containsKey('*')) {
            Node anyNode = TREE_MAP.get('*');
            for (int index = 0; index <= chars.length; index++)
                anyNode.find(chars, index);
        }
        return matchContext.MATCH_RESULT.toArray(MatchResult.EMPTY);
    }
}