package com.jd.jr.daat.dw.lineage.domains.lineage.column;

import com.alibaba.druid.sql.ast.SQLObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ColumnLineageRelationNode {
    /**
     * 父节点
     */
    private ColumnLineageColumnNode parent;

    /**
     * 子节点
     */
    private List<ColumnLineageColumnNode> children;

    /**
     * 关系节点类型
     */
    private ColumnLineageRelationNodeType relationNodeType;

    /**
     * 对应的 SQL Object
     */
    private SQLObject sqlObject;

    /**
     * 属性
     */
    private Map<String, String> properties;

    public ColumnLineageRelationNode(ColumnLineageRelationNodeType relationNodeType) {
        this(relationNodeType, new HashMap<String, String>());
    }

    public ColumnLineageRelationNode(ColumnLineageRelationNodeType relationNodeType, Map<String, String> properties) {
        this.relationNodeType = relationNodeType;
        this.properties = properties;
        this.children = Lists.newArrayList();
    }

    public void addChildNode(ColumnLineageColumnNode node) {
        children.add(node);
    }

    public void addChildreNodes(List<ColumnLineageColumnNode> nodes) {
        children.addAll(nodes);
    }

    public boolean hasChild() {
        return children.size() != 0;
    }

    public ColumnLineageColumnNode getFirstChild() {
        if (hasChild()) {
            return children.get(0);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "[R, type: %s]",
                relationNodeType.toString());
    }
}
