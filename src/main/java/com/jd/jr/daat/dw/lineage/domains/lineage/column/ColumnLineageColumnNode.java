package com.jd.jr.daat.dw.lineage.domains.lineage.column;

import com.alibaba.druid.sql.ast.SQLObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ColumnLineageColumnNode {
    /**
     * 父节点
     */
    private ColumnLineageRelationNode parent;

    /**
     * 子节点
     */
    private List<ColumnLineageRelationNode> children;

    /**
     * 字段节点类型
     */
    private ColumnLineageColumnNodeType columnNodeType;

    /**
     * 数据库名称
     */
    private String databaseName;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 字段名称
     */
    private String columnName;

    /**
     * 对应的 SQL Object
     */
    private SQLObject sqlObject;

    /**
     * 属性
     */
    private Map<String, String> properties;

    public ColumnLineageColumnNode(ColumnLineageColumnNodeType columnNodeType) {
        this(columnNodeType, new HashMap<String, String>());
    }

    public ColumnLineageColumnNode(ColumnLineageColumnNodeType columnNodeType, Map<String, String> properties) {
        this.columnNodeType = columnNodeType;
        this.properties = properties;
        this.children = Lists.newArrayList();

        this.databaseName = "UNKNOWN";
    }

    public void addChildNode(ColumnLineageRelationNode node) {
        children.add(node);
    }

    public void addChildrenNodes(List<ColumnLineageRelationNode> nodes) {
        children.addAll(nodes);
    }

    public boolean hasChild() {
        return children.size() != 0;
    }

    public ColumnLineageRelationNode getFirstChild() {
        if (hasChild()) {
            return children.get(0);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        if (columnNodeType == ColumnLineageColumnNodeType.STOP) {
            return "[C, STOP]";
        }

        return String.format(
                "[C, database: %s, table: %s, column: %s]",
                databaseName, tableName, columnName);
    }
}
