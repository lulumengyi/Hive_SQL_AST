package com.jd.jr.daat.dw.lineage.domains.lineage.column;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.domains.basic.Database;

import java.util.List;


public class ColumnLineageUtils {
    /**
     * Description:
     *   根据数据库名称获取字段级别血缘字段节点类型
     *
     * @param databaseName 数据库名称
     * @return 字段级别血缘字段节点类型
     */
    public static ColumnLineageColumnNodeType getColumnLineageColumnNodeTypeByDatabaseName(String databaseName) {
        ColumnLineageColumnNodeType columnNodeType = ColumnLineageColumnNodeType.HIVE_TABLE_COLUMN;

        for (String tmpDatabaseName : Database.HIVE_TMP_DATABASES) {
            if (databaseName.toUpperCase().equals(tmpDatabaseName)) {
                columnNodeType = ColumnLineageColumnNodeType.HIVE_TMP_TABLE_COLUMN;

                break;
            }
        }

        return columnNodeType;
    }

    /**
     * Description:
     *   根据子 Column 的 SQLObject 和父 Column 节点获取子 Column 节点类型
     *
     * @param sqlObject 子 Column 的 SQLObject
     * @param parentColumnNode 父 Column 节点
     * @return 子 Column 节点类型
     */
    public static ColumnLineageColumnNodeType getColumnLineageColumnNodeType(
            SQLObject sqlObject, ColumnLineageColumnNode parentColumnNode) {
        if (sqlObject instanceof SQLSubqueryTableSource) {
            return ColumnLineageColumnNodeType.HIVE_SUB_QUERY_TABLE_COLUMN;
        }

        if (sqlObject instanceof SQLAllColumnExpr) {
            return ColumnLineageColumnNodeType.HIVE_SUB_QUERY_TABLE_COLUMN_BY_ALL;
        }

        if (sqlObject instanceof SQLIdentifierExpr) {
            return ColumnLineageColumnNodeType.HIVE_SUB_QUERY_TABLE_COLUMN;
        }

        if(sqlObject instanceof SQLJoinTableSource){
            return ColumnLineageColumnNodeType.HIVE_SUB_QUERY_TABLE_COLUMN;
        }


        // 当前节点为物理表
        if (sqlObject instanceof SQLExprTableSource) {
            if (parentColumnNode.getTableName().equalsIgnoreCase("*")) {
                // 父节点为 SELECT *
                return ColumnLineageColumnNodeType.HIVE_TABLE_COLUMN_BY_ALL;
            } else {
                // 父节点为 SELECT item
                return ColumnLineageColumnNodeType.HIVE_TABLE_COLUMN;
            }
        }

        return ColumnLineageColumnNodeType.UNKNOWN;
    }

    /**
     * Description:
     *   根据根 Column 节点，得到 Lineage 路径
     *
     * @param rootColumnNode 跟 Column 节点
     * @return Lineage 路径
     */
    public static List<String> getFirstPathString(ColumnLineageColumnNode rootColumnNode) {
        StringBuilder path = new StringBuilder();

        List<String> result = Lists.newArrayList();

        ColumnLineageColumnNode currentColumnNode = rootColumnNode;

        List<String> start = Lists.newArrayList();
        start.add(currentColumnNode.getTableName() + "." + currentColumnNode.getColumnName());

        path.append("[START]");
        path.append(" -> ");

        while (currentColumnNode != null && currentColumnNode.hasChild()) {
            path.append(currentColumnNode.toString());
            path.append(" -> ");

            ColumnLineageRelationNode currentRelationNode = currentColumnNode.getFirstChild();
            path.append(currentRelationNode.toString());
            path.append(" -> ");

            currentColumnNode = currentRelationNode.getFirstChild();
        }
        path.append(currentColumnNode.toString());


        List<String> end = Lists.newArrayList();
        end.add(currentColumnNode.getTableName() + "." + currentColumnNode.getColumnName());

        path.append(" -> ");
        path.append("[END]");

        //避免首尾一样产生死循环
        if(!start.toString().equals(end.toString())){
            result.add(path.toString());
            result.add(start.toString());
            result.add(end.toString());
            return result;
        } else {
            return null;
        }
    }
}
