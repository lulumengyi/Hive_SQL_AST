package com.jd.jr.daat.dw.lineage.analysis;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.domains.basic.*;
import com.jd.jr.daat.dw.lineage.domains.lineage.column.*;
import com.jd.jr.daat.dw.lineage.domains.lineage.table.TableColumnLineage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnLineageAnalyzer {
    /**
     * Schema Repository
     */
    private SchemaRepository schemaRepository;

    public ColumnLineageAnalyzer(SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
    }

    /**
     * Description:
     *   添加不支持的节点
     *
     * @param columnNode 字段级别血缘字段节点
     * @param sqlObject SQLObject
     */
    public void putUnsupportedChildrenNodes(
            ColumnLineageColumnNode columnNode,
            SQLObject sqlObject) {
        String className = sqlObject.getClass().getName();

        // 构建 Relation 节点，类型 [UNSUPPORTED]
        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                ColumnLineageRelationNodeType.UNSUPPORTED);

        // 连接 Relation 节点和父 Column 节点
        relationChildNode.setParent(columnNode);
        columnNode.addChildNode(relationChildNode);

        // 构建子 Column 节点
        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(ColumnLineageColumnNodeType.STOP);

        // 连接 Relation 节点和子 Column 节点
        relationChildNode.addChildNode(columnChildNode);
        columnChildNode.setParent(relationChildNode);

        // 停止递归
        return;
    }
    /**
     * Description:
     *        添加 SQLPropertry 字段级别血缘
     *        为SQLCaseExpr设置
     *
     */
    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            SQLPropertyExpr sqlPropertyExpr) {

        // 构建 Relation 节点，类型 [DIRECT]
        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                ColumnLineageRelationNodeType.DIRECT);

        // 连接 Relation 节点和父 Column 节点
        relationChildNode.setParent(columnNode);
        columnNode.addChildNode(relationChildNode);
        // 获取子节点

        // 构建子 Column 节点
        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                .getColumnLineageColumnNodeType(sqlPropertyExpr.getResolvedOwnerObject(), parentColumnNode);
        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);
        columnChildNode.setSqlObject(sqlPropertyExpr.getResolvedOwnerObject());
        columnChildNode.setTableName(sqlPropertyExpr.getOwnernName());
        columnChildNode.setColumnName(sqlPropertyExpr.getName());
        // 连接 Relation 节点和子 Column 节点
        relationChildNode.addChildNode(columnChildNode);
        columnChildNode.setParent(relationChildNode);

        // 递归子 Column 节点

        putChildrenNodes(columnChildNode, columnNode, sqlPropertyExpr.getName());

    }


    /**
     * Description:
     *   添加 SQLSelectItem 字段级别血缘
     *
     * @param columnNode 字段级别血缘字段节点
     * @param parentColumnNode 父字段节点
     * @param sqlSelectItem SQLSelectItem (即 Column)
     */
    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            SQLSelectItem sqlSelectItem) {

        String columnNameOrAlias = null;
        String tablename =columnNode.getTableName();
        String databasename = columnNode.getDatabaseName();
        SQLObject sqlObject=null;
        if (sqlSelectItem.getExpr() instanceof SQLPropertyExpr) {
            // 构建 Relation 节点，类型 [DIRECT]
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.DIRECT);

            // 获取子节点
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlSelectItem.getExpr();

            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlPropertyExpr.getResolvedOwnerObject(), parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

            columnNameOrAlias = sqlPropertyExpr.getName();
            tablename =sqlPropertyExpr.getOwnernName();
            databasename =columnNode.getDatabaseName();
            sqlObject =sqlPropertyExpr.getResolvedOwnerObject();

            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

            // 递归子 Column 节点

            putChildrenNodes(columnChildNode, columnNode, sqlPropertyExpr.getName());
        } else if (sqlSelectItem.getExpr() instanceof SQLIdentifierExpr){
            // 构建 Relation 节点，类型 [DIRECT]
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.DIRECT);
            SQLExpr selectItemExpr = sqlSelectItem.getExpr();
            // 查询 Item 是 SELECT item 类型
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) selectItemExpr;

            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlIdentifierExpr.getResolvedOwnerObject(), parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);


            columnNameOrAlias = sqlIdentifierExpr.getName();
            sqlObject = sqlIdentifierExpr.getResolvedOwnerObject();

            //如果sqlobject为空，可能对应的schema中没有该字段
            if(sqlObject ==null){
                SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlIdentifierExpr.getParent().getParent();
                sqlObject = sqlSelectQueryBlock.getFrom();
            }

            //设置tablename和basename
            if (sqlObject instanceof SQLExprTableSource){
                SQLExpr expr = ((SQLExprTableSource) sqlObject).getExpr();
                tablename = GetTableDatabaseName(expr).split("\t")[0];
                databasename = GetTableDatabaseName(expr).split("\t")[1];
            } else if (sqlObject instanceof SQLUnionQueryTableSource){
                tablename = ((SQLUnionQueryTableSource) sqlObject).getAlias();
                databasename = columnNode.getDatabaseName();
            } else if (sqlObject instanceof SQLSubqueryTableSource){
                SQLObject sqlObject1= ((SQLSubqueryTableSource) sqlObject).getSelect().getQueryBlock().getFrom();

                if(sqlObject1 instanceof SQLExprTableSource){
                    SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) ((SQLSubqueryTableSource) sqlObject).getSelect().getQueryBlock().getFrom();
                    tablename = GetTableDatabaseName(sqlExprTableSource.getExpr()).split("\t")[0];
                    databasename = GetTableDatabaseName(sqlExprTableSource.getExpr()).split("\t")[1];
                } else if (sqlObject1 instanceof SQLSubqueryTableSource){
                    SQLObject sqlObject2 =((SQLSubqueryTableSource) sqlObject1).getSelect().getQueryBlock().getFrom();

                    if (sqlObject2 instanceof SQLExprTableSource){
                        tablename = GetTableDatabaseName(((SQLExprTableSource) sqlObject2).getExpr()).split("\t")[0];
                        databasename = GetTableDatabaseName(((SQLExprTableSource) sqlObject2).getExpr()).split("\t")[1];
                    }
                }

            }
            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode, columnChildNode, columnNode, columnNameOrAlias, databasename, tablename, sqlObject);

            putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);
        } else if(sqlSelectItem.getExpr() instanceof SQLCastExpr){
            //如果为SQLCastExpr类型

            // 构建 Relation 节点，类型 [FUNCTION]
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.CAST);

            SQLExpr selectItemExpr = sqlSelectItem.getExpr();
            SQLCastExpr sqlCastExpr = (SQLCastExpr) selectItemExpr;

            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlCastExpr, parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

            columnNameOrAlias =sqlSelectItem.getAlias();
            tablename=columnNode.getTableName();
            databasename =columnNode.getDatabaseName();
            sqlObject = null;

            //设置sqlObject
            if(sqlCastExpr.getExpr() instanceof  SQLPropertyExpr){
                columnNameOrAlias = ((SQLPropertyExpr) sqlCastExpr.getExpr()).getName();
                tablename =((SQLPropertyExpr) sqlCastExpr.getExpr()).getOwnernName();
                sqlObject =((SQLPropertyExpr) sqlCastExpr.getExpr()).getResolvedOwnerObject();
            }else if(sqlCastExpr.getExpr() instanceof  SQLIdentifierExpr){
                columnNameOrAlias = ((SQLIdentifierExpr) sqlCastExpr.getExpr()).getName();
                sqlObject = ((SQLIdentifierExpr) sqlCastExpr.getExpr()).getResolvedOwnerObject();
            }
            else if(sqlCastExpr.getExpr() instanceof SQLBinaryOpExpr){
                SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlCastExpr.getExpr();
                sqlBinaryOpExpr.getLeft();
            }else if(sqlCastExpr.getExpr() instanceof SQLAggregateExpr){

                SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) sqlCastExpr.getExpr();

                // 调用GetAggregateExprColunmnSQLObject函数 设置sqlAggregateExpr的sqlobject和columnNameOrAlias
                MethodInvoke SqlObjectColumnName =GetAggregateExprColunmnSQLObject(sqlAggregateExpr);
                sqlObject=SqlObjectColumnName.getSqlObject();
                columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();

            }else if(sqlCastExpr.getExpr() instanceof SQLMethodInvokeExpr){
                SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlCastExpr.getExpr();
                // 调用GetMethodInvokeColumnSQLObjec函数 得到sqlobect和columnNameOrAlias
                MethodInvoke SqlObjectColumnName =GetMethodInvokeColumnSQLObject(sqlMethodInvokeExpr);
                sqlObject=SqlObjectColumnName.getSqlObject();
                columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();

            }

            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

            putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

        }//如果为SQLAggregateExpr类型
        else if(sqlSelectItem.getExpr() instanceof SQLAggregateExpr){
            // 构建 Relation 节点，类型 [FUNCTION]
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.FUNCTION);

            SQLExpr selectItemExpr = sqlSelectItem.getExpr();
            // 查询 Item 是 SSQLAggregateExpr 类型
            SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) selectItemExpr;

            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlAggregateExpr, parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

            // 调用GetAggregateExprColunmnSQLObject函数 设置sqlAggregateExpr的sqlobject和columnNameOrAlias
            MethodInvoke SqlObjectColumnName =GetAggregateExprColunmnSQLObject(sqlAggregateExpr);
            sqlObject=SqlObjectColumnName.getSqlObject();
            columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
            //如果sqlobject为空
            if(sqlObject ==null){
                SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectItem.getParent();
                sqlObject = sqlSelectQueryBlock.getFrom();
            }
            tablename =columnNode.getTableName();
            databasename = columnNode.getDatabaseName();
            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

            putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

        }//如果是SQLMethodInvokeExpr函数嵌套关系
        else if(sqlSelectItem.getExpr() instanceof SQLMethodInvokeExpr){
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.FUNCTION);

            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr)sqlSelectItem.getExpr();
            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlMethodInvokeExpr, parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectItem.getParent();
            sqlObject = sqlSelectQueryBlock.getFrom();
            if(sqlObject instanceof SQLExprTableSource){
                tablename =GetTableDatabaseName(((SQLExprTableSource) sqlObject).getExpr()).split("\t")[0];
                databasename =GetTableDatabaseName(((SQLExprTableSource) sqlObject).getExpr()).split("\t")[1];

            }
            // 获取sqlobect和columnNameOrAlias
            MethodInvoke SqlObjectColumnName =GetMethodInvokeColumnSQLObject(sqlMethodInvokeExpr);
            sqlObject=SqlObjectColumnName.getSqlObject();
            columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
            if(columnNameOrAlias.equals("")){
                if(sqlSelectItem.getAlias()!=null){
                    columnNameOrAlias =sqlSelectItem.getAlias();
                }
            }

            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

            putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

        }//添加SQLCharExpr
        else if(sqlSelectItem.getExpr() instanceof SQLCharExpr){
            SQLCharExpr sqlCharExpr = (SQLCharExpr) sqlSelectItem.getExpr();
            columnNameOrAlias =sqlCharExpr.getText();
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.DIRECT);

            ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                    .getColumnLineageColumnNodeType(sqlCharExpr, parentColumnNode);
            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectItem.getParent();

            sqlObject = sqlSelectQueryBlock.getFrom();
            // 调用函数 连接childNode
            setColumnChildNode(relationChildNode, columnChildNode, columnNode, columnNameOrAlias, databasename, tablename, sqlObject);

        } else if (sqlSelectItem.getExpr() instanceof SQLBinaryOpExpr){
            // 构建 Relation 节点，类型 [FUNCTION]
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.FUNCTION);
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlSelectItem.getExpr();
            SQLExpr sqlLeftExpr =sqlBinaryOpExpr.getLeft();

            if (sqlLeftExpr instanceof SQLAggregateExpr){
                // 查询 Item 是 SSQLAggregateExpr 类型
                SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) sqlLeftExpr;

                ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                        .getColumnLineageColumnNodeType(sqlBinaryOpExpr, parentColumnNode);
                ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                // 调用GetAggregateExprColunmnSQLObject函数 设置sqlAggregateExpr的sqlobject和columnNameOrAlias
                MethodInvoke SqlObjectColumnName =GetAggregateExprColunmnSQLObject(sqlAggregateExpr);
                sqlObject=SqlObjectColumnName.getSqlObject();
                columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();

                //如果sqlobject为空
                if (sqlObject == null) {
                    SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectItem.getParent();
                    sqlObject = sqlSelectQueryBlock.getFrom();
                }

                tablename =columnNode.getTableName();
                databasename = columnNode.getDatabaseName();

                // 调用函数 连接childNode
                setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

            } else {
                putUnsupportedChildrenNodes(columnNode, sqlSelectItem);
            }
        } else {
            putUnsupportedChildrenNodes(columnNode, sqlSelectItem);
        }


    }

    /**
     * Description：得到SQLAggregateExpr类型中的sqlobject和columnNameOrAlias
     * @param
     * @return
     */
    public MethodInvoke GetExprColumnSQLObject(SQLExpr sqlExpr){
        //在这设置一个新类型，用来返回函数中的sqlobject和columnOrAlias
        MethodInvoke methodInvoke =new MethodInvoke();

        String columnNameOrAlias="";
        SQLObject sqlObject=null;
        //如果为SQLPropertyExpr类型
        if(sqlExpr instanceof SQLIdentifierExpr){
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) (sqlExpr);
            sqlObject = sqlIdentifierExpr.getResolvedOwnerObject();
            columnNameOrAlias = sqlIdentifierExpr.getName();
        }//如果为SQLPropertyExpr
        else if(sqlExpr instanceof SQLPropertyExpr){
            columnNameOrAlias = ((SQLPropertyExpr) sqlExpr).getName();
            sqlObject = ((SQLPropertyExpr) sqlExpr).getResolvedOwnerObject();
        }//如果是SQLMethodInvokeExpr类型
        else if(sqlExpr instanceof SQLMethodInvokeExpr){
            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlExpr;
            MethodInvoke SqlObjectColumnName =GetMethodInvokeColumnSQLObject(sqlMethodInvokeExpr);
            sqlObject=SqlObjectColumnName.getSqlObject();
            columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
        }//如果是SQLBinaryOpExpr类型
        else if(sqlExpr instanceof SQLBinaryOpExpr){
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLExpr sqlLeftExpr =sqlBinaryOpExpr.getLeft();
            MethodInvoke SqlObjectColumnName1 =GetExprColumnSQLObject(sqlLeftExpr);
            sqlObject=SqlObjectColumnName1.getSqlObject();
            columnNameOrAlias =SqlObjectColumnName1.getColumnNameOrAlias();
            if(columnNameOrAlias.equals("")){
                SQLExpr sqlRightExpr =sqlBinaryOpExpr.getRight();
                MethodInvoke SqlObjectColumnName2 =GetExprColumnSQLObject(sqlRightExpr);
                sqlObject=SqlObjectColumnName2.getSqlObject();
                columnNameOrAlias =SqlObjectColumnName2.getColumnNameOrAlias();
            }
        }
        //如果是SQLCaseExpr类型
        else if(sqlExpr instanceof SQLCaseExpr){
            SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) sqlExpr;
            if(sqlCaseExpr.getValueExpr() !=null){
                MethodInvoke SqlObjectColumnName =GetExprColumnSQLObject(sqlCaseExpr.getValueExpr() );
                sqlObject=SqlObjectColumnName.getSqlObject();
                columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
            }

            if(sqlCaseExpr.getItems().size()!=0){
                SQLExpr sqlExpr1 =sqlCaseExpr.getItems().get(0).getValueExpr();
                if(sqlCaseExpr.getItems().get(0).getConditionExpr() instanceof SQLBinaryOpExpr){
                    SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlCaseExpr.getItems().get(0).getConditionExpr();
                    MethodInvoke SqlObjectColumnName1 =GetExprColumnSQLObject(sqlBinaryOpExpr);
                    sqlObject=SqlObjectColumnName1.getSqlObject();
                    columnNameOrAlias =SqlObjectColumnName1.getColumnNameOrAlias();
                }
            }
        }
        methodInvoke.setColumnNameOrAlias(columnNameOrAlias);
        methodInvoke.setSqlObject(sqlObject);
        return methodInvoke;
    }


    public MethodInvoke GetAggregateExprColunmnSQLObject(SQLAggregateExpr sqlAggregateExpr){
        //在这设置一个新类型，用来返回函数中的sqlobject和columnOrAlias
        MethodInvoke methodInvoke =new MethodInvoke();

        String columnNameOrAlias="";
        SQLObject sqlObject=null;
        //找到字段和对应的sqlobject
        if(sqlAggregateExpr.getArguments().size()!=0){
            SQLExpr sqlExpr =sqlAggregateExpr.getArguments().get(0);
            MethodInvoke SqlObjectColumnName =GetExprColumnSQLObject(sqlExpr);
            sqlObject=SqlObjectColumnName.getSqlObject();
            columnNameOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
        }
        methodInvoke.setColumnNameOrAlias(columnNameOrAlias);
        methodInvoke.setSqlObject(sqlObject);

        return methodInvoke;


    }

    /**
     * Description:解决函数嵌套问题,循环得到最后的字段 如nvl(round(sum(awardvolumn),5),0) 得到awardvolumn这个字段列名和对应的sqlobject
     * @param sqlMethodInvokeExpr 函数嵌套表达式
     */
    public MethodInvoke GetMethodInvokeColumnSQLObject(SQLMethodInvokeExpr sqlMethodInvokeExpr){
        //在这设置一个新类型，用来返回嵌套函数中的sqlobject和columnOrAlias
        MethodInvoke methodInvoke =new MethodInvoke();
        //得到第一个嵌套函数中的字段，如nvl(round(sum(awardvolumn),5),0)，得到round(sum(awardvolumn),5)
        int parametersSize=sqlMethodInvokeExpr.getParameters().size();
        //初始化sqlObject和columnOrAlias
        SQLObject sqlObject = null;
        String columnOrAlias ="";
        //循环得到最后一个字段
        while(parametersSize!=0){
            SQLExpr  SQLparameter = sqlMethodInvokeExpr.getParameters().get(parametersSize-1);
            MethodInvoke SqlObjectColumnName =GetExprColumnSQLObject(SQLparameter);
            sqlObject=SqlObjectColumnName.getSqlObject();
            columnOrAlias =SqlObjectColumnName.getColumnNameOrAlias();
            if(sqlObject!=null && !columnOrAlias.equals("")){
                break;
            }
            else{
                parametersSize-=1;
                continue;
            }
        }

        methodInvoke.setSqlObject(sqlObject);
        methodInvoke.setColumnNameOrAlias(columnOrAlias);
        return methodInvoke;
    }

    /**
     *Decription: 连接节点，将此操作合并为函数，此后都调用此函数即可。
     * @param relationChildNode
     * @param columnChildNode
     * @param columnNode
     * @param columnNameOrAlias
     * @param databasename
     * @param tablename
     * @param sqlObject
     */
    public void setColumnChildNode( ColumnLineageRelationNode relationChildNode,ColumnLineageColumnNode columnChildNode,ColumnLineageColumnNode columnNode,
                               String columnNameOrAlias, String databasename,String tablename,SQLObject sqlObject){
        // 连接 Relation 节点和父 Column 节点
        relationChildNode.setParent(columnNode);
        columnNode.addChildNode(relationChildNode);

        // 构建子 Column 节点
        columnChildNode.setSqlObject(sqlObject);
        columnChildNode.setTableName(tablename);
        columnChildNode.setDatabaseName(databasename);
        columnChildNode.setColumnName(columnNameOrAlias);

        // 连接 Relation 节点和子 Column 节点
        relationChildNode.addChildNode(columnChildNode);
        columnChildNode.setParent(relationChildNode);

    }

    /**
     * Decription:将SQLTableSource各种类型 都调用此函数
     * @param columnNode 节点
     * @param parentColumnNode 父节点
     * @param sqlTableSource sqlTableSource
     * @param nearestAncestorColumnSpecificName 最近的节点，判断是否与selectItem是否相同
     * @param selectItems selectItems
     */

    public void putChildrenNodes( ColumnLineageColumnNode columnNode,
                                     ColumnLineageColumnNode parentColumnNode,SQLTableSource sqlTableSource,
                                     String nearestAncestorColumnSpecificName,List<SQLSelectItem> selectItems){
        //如果为SQLExprTableSource类型
        if(sqlTableSource instanceof SQLExprTableSource) {
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            SQLExpr expr = sqlExprTableSource.getExpr();
            String tablename = GetTableDatabaseName(expr).split("\t")[0];
            String databasename = GetTableDatabaseName(expr).split("\t")[1];
            SQLObject sqlObject =sqlExprTableSource;
            for (SQLSelectItem selectItem : selectItems) {
                SQLExpr selectItemExpr = selectItem.getExpr();

                if (selectItemExpr instanceof SQLPropertyExpr) {
                    // 查询 Item 是一般查询项
                    SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) selectItemExpr;

                    // 获取字段名或别名
                    String columnNameOrAlias = sqlPropertyExpr.getName();
                    if (selectItem.getAlias() != null) {
                        columnNameOrAlias = selectItem.getAlias();
                    }
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        // 构建 Relation 节点，类型 [DIRECT]
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.DIRECT);
                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlPropertyExpr.getResolvedOwnerObject(), parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        columnNameOrAlias =sqlPropertyExpr.getName();

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        // 递归子 Column 节点
                        putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

                        // 发现同名 (字段名或别名相同)，即退出
                        break;
                    }
                    //添加select APF.*的情况
                    else if (columnNameOrAlias.equals("*")) {
                        // 查询 Item 是 SELECT * 类型
//                    SQLAllColumnExpr sqlAllColumnExpr = (SQLAllColumnExpr) selectItemExpr;

                        // 构建 Relation 节点，类型 [DIRECT]
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.DIRECT);
                        // 构建子 Column 节点
                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlPropertyExpr.getResolvedOwnerObject(), parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        columnNameOrAlias ="*";

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        // 递归子 Column 节点
                        putChildrenNodes(columnChildNode, columnNode, nearestAncestorColumnSpecificName);
                        // 找到则跳出
                        break;

                    }
                } else if (selectItemExpr instanceof SQLIdentifierExpr) {
                    // 查询 Item 是 SELECT item 类型
                    SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) selectItemExpr;

                    // 获取字段名或别名
                    String columnNameOrAlias = sqlIdentifierExpr.getName();
                    if (selectItem.getAlias() != null) {
                        columnNameOrAlias = selectItem.getAlias();
                    }

                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        // 构建 Relation 节点，类型 [DIRECT]
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.DIRECT);
                        // 构建子 Column 节点
                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlIdentifierExpr, parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        // 递归子 Column 节点
                        putChildrenNodes(columnChildNode, columnNode, nearestAncestorColumnSpecificName);

                        // 找到则跳出
                        break;
                    }
                } //如果为SQLAllColumnExpr类型
                else if (selectItemExpr instanceof SQLAllColumnExpr) {
                    // 查询 Item 是 SELECT * 类型
                    SQLAllColumnExpr sqlAllColumnExpr = (SQLAllColumnExpr) selectItemExpr;

                    // 构建 Relation 节点，类型 [DIRECT]
                    ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                            ColumnLineageRelationNodeType.DIRECT);

                    // 构建子 Column 节点
                    ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                            .getColumnLineageColumnNodeType(sqlAllColumnExpr, parentColumnNode);
                    ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);
                    String columnNameOrAlias ="*";

                    setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                    putChildrenNodes(columnChildNode, columnNode, nearestAncestorColumnSpecificName);
                    // 找到则跳出
                    break;
                } //如果为SQLMethodInvokeExpr函数嵌套类型
                else if (selectItemExpr instanceof SQLMethodInvokeExpr) {

                    String columnNameOrAlias = selectItemExpr.toString();
                    if (selectItem.getAlias() != null) {
                        columnNameOrAlias = selectItem.getAlias();
                    }
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.FUNCTION);
                        // 连接 Relation 节点和父 Column 节点

                        SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) selectItem.getExpr();

                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlMethodInvokeExpr, parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        putChildrenNodes(columnChildNode, columnNode, nearestAncestorColumnSpecificName);
                        break;
                    }
                } //如果为SQLAggregateExpr函数类型
                else if (selectItemExpr instanceof SQLAggregateExpr) {

                    // 查询 Item 是 SSQLAggregateExpr 类型
                    SQLAggregateExpr sqlAggregateExpr = (SQLAggregateExpr) selectItemExpr;

                    // 获取方法名
                    String methodName = sqlAggregateExpr.getMethodName();
                    String columnNameOrAlias = selectItemExpr.toString();
                    if (selectItem.getAlias() != null) {
                        columnNameOrAlias = selectItem.getAlias();
                    }
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        // 构建 Relation 节点，类型 [FUNCTION]
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.FUNCTION);

                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlAggregateExpr, parentColumnNode);

                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);
                        break;

                    }
                } //如果为SQLCharExpr类型
                else if(selectItemExpr instanceof SQLCharExpr){
                    String columnNameOrAlias = selectItem.getAlias();
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        SQLCharExpr sqlCharExpr = (SQLCharExpr) selectItemExpr;
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.DIRECT);

                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlCharExpr, parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);
                        break;
                    }
                }//如果为SQLCaseExpr类型
                else if (selectItemExpr instanceof SQLCaseExpr) {
                    String columnNameOrAlias = selectItem.getAlias();
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        SQLCaseExpr sqlCaseExpr = (SQLCaseExpr) selectItemExpr;
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.CASE);

                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(sqlCaseExpr, parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);


                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);
                        break;
                    }
                } //添加如：COALESCE(buyer_frozen,0)+COALESCE(erp_frozen,0) AS frozen 有运算符的表达式
                else if(selectItemExpr instanceof SQLBinaryOpExpr){
                    String columnNameOrAlias = selectItem.getAlias();
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                                ColumnLineageRelationNodeType.FUNCTION);

                        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                                .getColumnLineageColumnNodeType(selectItemExpr, parentColumnNode);
                        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

                        setColumnChildNode(relationChildNode,columnChildNode,columnNode,columnNameOrAlias,databasename,tablename,sqlObject);

                        putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);
                        break;
                    }
                }
                else {
                    String columnNameOrAlias = selectItem.getAlias();
                    if (nearestAncestorColumnSpecificName.equalsIgnoreCase(columnNameOrAlias)) {
                        putUnsupportedChildrenNodes(columnNode, selectItemExpr);
                    }

                }
            }

        }//如果为SQLSubqueryTableSource类型
        else if(sqlTableSource instanceof SQLSubqueryTableSource){
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            if(sqlSubqueryTableSource.getSelect().getQuery() instanceof SQLSelectQueryBlock){
                SQLSelectQueryBlock sqlSelectQueryBlock1 = (SQLSelectQueryBlock) sqlSubqueryTableSource.getSelect().getQuery();
                SQLTableSource sqlTableSource1 =sqlSelectQueryBlock1.getFrom();
                putChildrenNodes(columnNode,parentColumnNode,sqlTableSource1,nearestAncestorColumnSpecificName,selectItems);
            }else if(sqlSubqueryTableSource.getSelect().getQuery() instanceof SQLUnionQuery){
                SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSubqueryTableSource.getSelect().getQuery();
                SQLSelectQuery sqlSelectQuery =sqlUnionQuery.getLeft();
            }

        }//如果为SQLJoinTableSource类型
        else if(sqlTableSource instanceof SQLJoinTableSource){
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlTableSource;
            SQLTableSource sqlTableSourceLeft =sqlJoinTableSource.getLeft();
            SQLTableSource sqlTableSourceRight =sqlJoinTableSource.getRight();
            if(sqlTableSourceLeft instanceof SQLSubqueryTableSource){
                SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSourceLeft;
                putChildrenNodes(columnNode,parentColumnNode,sqlSubqueryTableSource ,nearestAncestorColumnSpecificName,selectItems);
            }
            else if(sqlTableSourceLeft instanceof SQLExprTableSource){
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSourceLeft;
                putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlExprTableSource);
            }else if(sqlTableSourceLeft instanceof SQLJoinTableSource){
                SQLJoinTableSource sqlJoinTableSource1 = (SQLJoinTableSource) sqlTableSourceLeft;
                putChildrenNodes(columnNode,parentColumnNode,sqlJoinTableSource1 ,nearestAncestorColumnSpecificName,selectItems);

            }

        }//如果为SQLUnionQueryTableSource类型
        else if(sqlTableSource instanceof SQLUnionQueryTableSource){
            SQLUnionQueryTableSource sqlUnionQueryTableSource = (SQLUnionQueryTableSource)sqlTableSource;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlUnionQueryTableSource);
        }

    }
    /**
     * Decription:SQLUnionQueryTableSource类型

     */

    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            String nearestAncestorColumnSpecificName,
            SQLUnionQueryTableSource sqlUnionQueryTableSource) {
        SQLUnionQuery sqlUnionQuery = sqlUnionQueryTableSource.getUnion();
        putSQLUnionQuery(columnNode,parentColumnNode,nearestAncestorColumnSpecificName,sqlUnionQuery);

    }

    /**
     * Desciprition: 对SQLUnionQuery进行解析
     * @param columnNode
     * @param parentColumnNode
     * @param nearestAncestorColumnSpecificName
     * @param sqlUnionQuery
     */

    public void putSQLUnionQuery(  ColumnLineageColumnNode columnNode,
                                   ColumnLineageColumnNode parentColumnNode,
                                   String nearestAncestorColumnSpecificName,
                                   SQLUnionQuery sqlUnionQuery){
        List<SQLSelectItem> selectItemList = new ArrayList<>();
        selectItemList = GetSQLUnionQuerySelectItemList(sqlUnionQuery,selectItemList);
        if(sqlUnionQuery.getLeft() instanceof SQLSelectQueryBlock){
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlUnionQuery.getLeft();
            SQLTableSource sqlTableSource =sqlSelectQueryBlock.getFrom();
            putChildrenNodes(columnNode,parentColumnNode,sqlTableSource,nearestAncestorColumnSpecificName,selectItemList);
        }else if(sqlUnionQuery.getRight() instanceof SQLSelectQueryBlock){
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlUnionQuery.getRight();
            SQLTableSource sqlTableSource =sqlSelectQueryBlock.getFrom();
            putChildrenNodes(columnNode,parentColumnNode,sqlTableSource,nearestAncestorColumnSpecificName,selectItemList);
        }else if(sqlUnionQuery.getLeft() instanceof SQLUnionQuery){
            SQLUnionQuery sqlUnionQuery1 = (SQLUnionQuery) sqlUnionQuery.getLeft();
            putSQLUnionQuery(columnNode,parentColumnNode,nearestAncestorColumnSpecificName,sqlUnionQuery1);
        }else if (sqlUnionQuery.getRight() instanceof SQLUnionQuery){
            SQLUnionQuery sqlUnionQuery1 = (SQLUnionQuery) sqlUnionQuery.getRight();
            putSQLUnionQuery(columnNode,parentColumnNode,nearestAncestorColumnSpecificName,sqlUnionQuery1);

        }

    }



    /**
     * Description:
     *   SQLSubqueryTableSource 类型添加子查询字段级别血缘
     *
     * @param columnNode 字段级别血缘字段节点
     * @param parentColumnNode 父字段节点
     * @param nearestAncestorColumnSpecificName 最近的祖先节点的明确的列名
     * @param sqlSubqueryTableSource 子查询 SQLObject
     */
    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            String nearestAncestorColumnSpecificName,
            SQLSubqueryTableSource sqlSubqueryTableSource) {
        if(sqlSubqueryTableSource.getSelect().getQuery() instanceof SQLSelectQueryBlock){
            SQLSelectQueryBlock sqlSelectQueryBlock =sqlSubqueryTableSource.getSelect().getQueryBlock();
            List<SQLSelectItem> selectItems =sqlSelectQueryBlock.getSelectList();
            SQLTableSource sqlTableSource =sqlSelectQueryBlock.getFrom();
            putChildrenNodes(columnNode,parentColumnNode,sqlTableSource,nearestAncestorColumnSpecificName,selectItems);

        }

    }

    /**
     * Description:
     *   添加物理表查询字段级别血缘
     *
     * @param columnNode 字段级别血缘字段节点
     * @param parentColumnNode 父字段节点
     * @param nearestAncestorColumnSpecificName 最近的祖先节点的明确的列名
     * @param sqlExprTableSource 物理表查询 SQLObject
     */
    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            String nearestAncestorColumnSpecificName,
            SQLExprTableSource sqlExprTableSource) {
        // 构建 Relation 节点，类型 [DIRECT]
        ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                ColumnLineageRelationNodeType.DIRECT);
        relationChildNode.setParent(columnNode);
        columnNode.addChildNode(relationChildNode);

        // 构建子 Column 节点
        ColumnLineageColumnNodeType columnChildNodeType = ColumnLineageUtils
                .getColumnLineageColumnNodeType(sqlExprTableSource, parentColumnNode);
        ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnChildNodeType);

        // 设置属性
        SQLExpr sqlExprTableSourceExpr = sqlExprTableSource.getExpr();

        if (sqlExprTableSourceExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExprTableSourceExpr;

            columnChildNode.setSqlObject(sqlPropertyExpr.getResolvedOwnerObject());
            columnChildNode.setDatabaseName(sqlPropertyExpr.getOwnernName());
            columnChildNode.setTableName(sqlPropertyExpr.getName());
        }else if(sqlExprTableSourceExpr instanceof SQLIdentifierExpr){
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExprTableSourceExpr;
            columnChildNode.setTableName(sqlIdentifierExpr.getName());
            columnChildNode.setDatabaseName(columnNode.getDatabaseName());
            columnChildNode.setSqlObject(sqlIdentifierExpr.getResolvedOwnerObject());
        }

        columnChildNode.setColumnName(nearestAncestorColumnSpecificName);

        // 连接 Relation 节点和子 Column 节点
        relationChildNode.addChildNode(columnChildNode);
        columnChildNode.setParent(relationChildNode);

        // 结束递归
        return;
    }

    /**
     * Description:
     *   根据字段级别血缘字段节点和父字段节点添加子节点
     *
     * @param columnNode 字段级别血缘字段节点
     * @param parentColumnNode 父字段节点
     * @param nearestAncestorColumnSpecificName 最近的祖先节点的明确的列名
     */
    public void putChildrenNodes(
            ColumnLineageColumnNode columnNode,
            ColumnLineageColumnNode parentColumnNode,
            String nearestAncestorColumnSpecificName) {
        SQLObject sqlObject = columnNode.getSqlObject();
        if (sqlObject instanceof SQLSelectItem) {
            // 直接查询
            SQLSelectItem sqlSelectItem = (SQLSelectItem) sqlObject;
            SQLExpr sqlExpr =sqlSelectItem.getExpr();
            putChildrenNodes(columnNode, parentColumnNode, sqlSelectItem);
        }
        else if(sqlObject instanceof  SQLPropertyExpr){
            SQLPropertyExpr sqlPropertyExpr =(SQLPropertyExpr) sqlObject;
            putChildrenNodes(columnNode, parentColumnNode, sqlPropertyExpr);
        }
        else if (sqlObject instanceof SQLSubqueryTableSource) {
            // 子查询
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlObject;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlSubqueryTableSource);

        } else if (sqlObject instanceof SQLExprTableSource) {
            // 物理表查询
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlObject;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlExprTableSource);
        }//增加SQLUnionQueryTableSource类型
        else if(sqlObject  instanceof  SQLUnionQueryTableSource){
            SQLUnionQueryTableSource sqlUnionQueryTableSource = (SQLUnionQueryTableSource) sqlObject;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlUnionQueryTableSource);
        }//SQLJoinTableSource类型
        else if(sqlObject instanceof  SQLJoinTableSource){
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlObject;
        }
    }

    /**
     * Description ：
     * @param columnNode
     * @param parentColumnNode
     * @param nearestAncestorColumnSpecificName
     * @param sqlJoinTableSource
     */

    public void putChildrenNodes( ColumnLineageColumnNode columnNode,
                                  ColumnLineageColumnNode parentColumnNode,
                                  String nearestAncestorColumnSpecificName,
                                  SQLJoinTableSource sqlJoinTableSource){
        SQLTableSource sqlTableSourceLeft =sqlJoinTableSource.getLeft();
        SQLTableSource sqlTableSourceRight =sqlJoinTableSource.getRight();
        //如果为SQLSubqueryTableSource类型
        if(sqlTableSourceLeft instanceof SQLSubqueryTableSource){
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSourceLeft;
            if(sqlSubqueryTableSource.getSelect().getQuery() instanceof SQLSelectQueryBlock) {
                putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlSubqueryTableSource);
            }

        }//如果为SQLExprTableSource
        else if(sqlTableSourceLeft instanceof SQLExprTableSource){
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSourceLeft;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName, sqlExprTableSource);
        }//如果为SQLJoinTableSource
        else if(sqlTableSourceLeft instanceof SQLJoinTableSource){
            SQLJoinTableSource sqlJoinTableSource1 = (SQLJoinTableSource) sqlTableSourceLeft;
            putChildrenNodes(columnNode, parentColumnNode, nearestAncestorColumnSpecificName,sqlJoinTableSource1);
        }



    }

    /** Description:
     *   添加别名关系的节点
     *      @param columnNode 表的字段级别血缘
     *      @param sqlSelectItem SQLSelectItem (即 Column)
     *      @param table 对应的表
     *
     */
    public void putSQLSelectItemAliasNode(ColumnLineageColumnNode columnNode, ColumnLineageColumnNode parentColumnNode,
                                          SQLSelectItem sqlSelectItem,Table table){
        if (sqlSelectItem.getAlias()!=null){
            SQLExpr selectItemExpr = sqlSelectItem.getExpr();
            String columnNameOrAlias = selectItemExpr.toString();
            ColumnLineageRelationNode relationChildNode = new ColumnLineageRelationNode(
                    ColumnLineageRelationNodeType.ALIAS);
            ColumnLineageColumnNodeType columnNodeType =
                    ColumnLineageUtils.getColumnLineageColumnNodeTypeByDatabaseName(table.getDatabase().getName());

            ColumnLineageColumnNode columnChildNode = new ColumnLineageColumnNode(columnNodeType);
            columnChildNode.setSqlObject(sqlSelectItem);
            columnChildNode.setDatabaseName(table.getDatabase().getName());
            columnChildNode.setTableName(table.getName());

            //添加SQLCaseExpr类型
            if(selectItemExpr instanceof SQLCaseExpr){
                relationChildNode = new ColumnLineageRelationNode(
                        ColumnLineageRelationNodeType.CASE);
                //避免首尾一致，暂时不解析case情况
                columnNameOrAlias = "CASE_"+sqlSelectItem.getAlias();
                columnChildNode.setSqlObject(null);

            }
            // 连接 Relation 节点和父 Column 节点
            relationChildNode.setParent(columnNode);
            columnNode.addChildNode(relationChildNode);
            // 获取子节点

            columnChildNode.setColumnName(columnNameOrAlias);

            relationChildNode.addChildNode(columnChildNode);
            columnChildNode.setParent(relationChildNode);
            putChildrenNodes(columnChildNode, columnNode, columnNameOrAlias);

        }
    }


    /**
     * Description:
     *   获取单个 SQLSelectItem (即 Column) 的血缘
     *
     * @param tableColumnLineage 表的字段级别血缘
     * @param selectItem SQLSelectItem (即 Column)
     * @param table 对应的表
     * @return 表的字段级别血缘
     */
    public void putSQLSelectItemColumnLineage(
            TableColumnLineage tableColumnLineage, SQLSelectItem selectItem, Table table) {
        //首先构建列名的节点，再构建列名的别名节点
        SQLExpr selectItemExpr = selectItem.getExpr();
        String columnNameOrAlias = "";
        SQLObject sqlObject =selectItem;

        //如果别名不为空，我们先建一个别名的节点，再建一个原始名的节点。如 SUM(A.count) AS B 先建B节点再建SUM(A.count)
        if (selectItemExpr instanceof SQLPropertyExpr) {
            columnNameOrAlias = ((SQLPropertyExpr) selectItem.getExpr()).getName();
        }else if(selectItemExpr instanceof SQLIdentifierExpr){
            columnNameOrAlias = ((SQLIdentifierExpr) selectItemExpr).getName();
        }//如果是caseExpr
        else if(selectItemExpr instanceof SQLCaseExpr){
            columnNameOrAlias = ((SQLCaseExpr) selectItemExpr).getItems().get(0).getValueExpr().toString();
        }//select *情况
        else if (selectItemExpr instanceof SQLAllColumnExpr){
            if(((SQLAllColumnExpr) selectItemExpr).getResolvedTableSource() instanceof SQLExprTableSource){
                SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) ((SQLAllColumnExpr) selectItemExpr).getResolvedTableSource();
                columnNameOrAlias = "*";
                sqlObject =sqlExprTableSource;
            }else if(((SQLAllColumnExpr) selectItemExpr).getResolvedTableSource() instanceof SQLSubqueryTableSource){
                SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) ((SQLAllColumnExpr) selectItemExpr).getResolvedTableSource();
                sqlObject = sqlSubqueryTableSource;
            }

        }
        if(selectItem.getAlias()!=null){
            columnNameOrAlias = selectItem.getAlias();
        }

        Column column = new Column(columnNameOrAlias, ColumnType.HIVE_COLUMN, table);
        tableColumnLineage.addColumn(column);
        ColumnLineageColumnNodeType columnNodeType =
                ColumnLineageUtils.getColumnLineageColumnNodeTypeByDatabaseName(table.getDatabase().getName());
        ColumnLineageColumnNode columnRootNode = new ColumnLineageColumnNode(columnNodeType);

        columnRootNode.setSqlObject(sqlObject);

        columnRootNode.setDatabaseName(table.getDatabase().getName());
        columnRootNode.setTableName(table.getName());
        columnRootNode.setColumnName(columnNameOrAlias);

        tableColumnLineage.addColumnLineageColumnRootNode(column, columnRootNode);
        if(selectItem.getAlias()==null){
            putChildrenNodes(columnRootNode, columnRootNode, columnNameOrAlias);
        }//如果别名不为空，再建原名节点
        else{
            putSQLSelectItemAliasNode(columnRootNode,columnRootNode,selectItem,table);
        }

    }

    /**
     * Description:
     *   获取 SQLStatement 中表的字段级别血缘
     *
     * @param tableColumnLineage 表的字段级别血缘
     * @param stmt SQLStatement
     */
    public String putSQLStatementColumnLineage(TableColumnLineage tableColumnLineage, SQLStatement stmt,String databaseName) {
        //从use语句中获得basename
        String tableName ="";
        if (stmt instanceof  SQLUseStatement){
            databaseName= ((SQLUseStatement) stmt).getDatabase().getSimpleName();
            return databaseName;
        }
        // 仅针对有建表语句的 SQL 进行分析
        if (stmt instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement stmtCT = (SQLCreateTableStatement) stmt;

            tableName =GetTableDatabaseName(stmtCT.getTableSource().getExpr()).split("\t")[0];
            if(databaseName.equals("")){
                databaseName =GetTableDatabaseName(stmtCT.getTableSource().getExpr()).split("\t")[1];
            }
            Database database = new Database(databaseName, DatabaseType.HIVE_DB);
            Table table = new Table(tableName, TableType.HIVE_TABLE, database);

            // 仅针对包含 SELECT 的建表 SQL 进行分析
            SQLSelect select = stmtCT.getSelect();

            if (select != null) {
                List<SQLSelectItem> selectItemList=new ArrayList<>();
                selectItemList = GetSQLUnionQuerySelectItemList(select.getQuery(),selectItemList);
                //增加SQLUnionQuery类
                for (SQLSelectItem selectItem : selectItemList) {
                    putSQLSelectItemColumnLineage(tableColumnLineage, selectItem, table);
                }
            }
        }//SQLInsertStatement类型
        else if (stmt instanceof SQLInsertStatement){
            SQLInsertStatement stmtInsert = (SQLInsertStatement) stmt;
            tableName =GetTableDatabaseName(stmtInsert.getTableSource().getExpr()).split("\t")[0];
            if(databaseName.equals("")){
                databaseName =GetTableDatabaseName(stmtInsert.getTableSource().getExpr()).split("\t")[1];
            }
            Database database = new Database(databaseName, DatabaseType.HIVE_DB);
            Table table = new Table(tableName, TableType.HIVE_TABLE, database);
            List<SQLSelectItem> selectItemList=new ArrayList<>() ;
            //增加SQLUnionQuery类型
            if(stmtInsert.getValuesList().size()==0){
                selectItemList = GetSQLUnionQuerySelectItemList(stmtInsert.getQuery().getQuery(),selectItemList);
            }
            if(selectItemList!=null){
                for (SQLSelectItem selectItem : selectItemList) {
                    putSQLSelectItemColumnLineage(tableColumnLineage, selectItem, table);
                }
            }

        }
        return databaseName;
    }

    /**
     * Description:
     *   获取单个 SQL 中表的字段级别血缘
     *
     * @param sql SQL
     * @return 表的字段级别血缘
     */
    public TableColumnLineage getTableColumnLineage(String sql,String filename) throws IOException {
        TableColumnLineage tableColumnLineage = new TableColumnLineage();
        String databaseName="";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, schemaRepository.getDbType());

        stmtList.forEach(schemaRepository::resolve);

            for (SQLStatement stmt : stmtList) {
                if (stmt instanceof  SQLUseStatement){
                    databaseName= ((SQLUseStatement) stmt).getDatabase().getSimpleName();
                }
                System.out.println("Dealing with one SQL statement ...");
                //CreateSchemaTable(stmt, filename,databaseName);
                databaseName = putSQLStatementColumnLineage(tableColumnLineage, stmt, databaseName);
            }
        return tableColumnLineage;

    }

    /**
     * Description:
     *    创建上一个新建表的schema信息
     * @param  stmt SQLObject filename 文件
     * @return tableColumnLineages
     */

    public String CreateSchemaTable(SQLStatement stmt,String filename,String databaseName) throws IOException {
        //将生成的table schema SQL 写到 文件中
        File file=new File(filename);
        FileWriter writer = new FileWriter(file, true);
        String tableName = null;
        StringBuffer CreateTableSQL= new StringBuffer();
        int flag=0;
        if (stmt instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement stmtCT = (SQLCreateTableStatement) stmt;
            if (stmtCT.getTableSource().getExpr() instanceof SQLPropertyExpr){
                tableName = ((SQLPropertyExpr) stmtCT.getTableSource().getExpr()).getName();
                databaseName = ((SQLPropertyExpr) stmtCT.getTableSource().getExpr()).getOwnernName();
                String sql = "CREATE TABLE IF NOT EXISTS "+databaseName+"."+tableName+"\n"+"(";
                CreateTableSQL.append(sql);
            }else if(stmtCT.getTableSource().getExpr() instanceof  SQLIdentifierExpr){
                tableName = ((SQLIdentifierExpr) stmtCT.getTableSource().getExpr()).getName();
                String sql = "CREATE TABLE IF NOT EXISTS "+ databaseName + "."+tableName+"\n"+"(";
                CreateTableSQL.append(sql);
            }
            if (stmtCT.getTableSource().getSchemaObject()==null) {
                SQLSelect select = stmtCT.getSelect();
                if (select != null) {
                    flag =1;
                    List<SQLSelectItem> selectItemList =new ArrayList<>();
                    //增加SQLUnionQuery类型
                    selectItemList =GetSQLUnionQuerySelectItemList(select.getQuery(),selectItemList);
                    selectItemListToFile(selectItemList, CreateTableSQL, writer);
                }//只有create语句
                else {
                    List<SQLTableElement> sqlTableElements =stmtCT.getTableElementList();
                    if(sqlTableElements.size()!=0){
                        flag =1;
                        for(SQLTableElement sqlTableElement:sqlTableElements ){
                            if(sqlTableElement instanceof  SQLColumnDefinition){
                                SQLName sqlName = ((SQLColumnDefinition) sqlTableElement).getName();
                                CreateTableSQL.append(sqlName.getSimpleName() +" STRING"+ ","+"\n");
                            }
                        }
                    }

                }
            }//只有在select不为空的情况下写入
            if(flag ==1){
                CreateTableSQL.append(")");
                if(((SQLCreateTableStatement) stmt).getPartitionColumns().size()!=0){
                    int size = ((SQLCreateTableStatement) stmt).getPartitionColumns().size();
                    CreateTableSQL.append("partitioned by (");
                    while (size!=0){
                        String partitionColumn =((SQLCreateTableStatement) stmt).getPartitionColumns().get(size-1).toString();
                        if(size ==1){
                            CreateTableSQL.append(partitionColumn);
                        }else{
                            CreateTableSQL.append(partitionColumn+",");
                        }
                        size-=1;
                    }
                    CreateTableSQL.append(");");
                }
                else{
                    CreateTableSQL.append(";");
                }
                writer.write(CreateTableSQL.toString()+"\n");
                writer.close();
            }

        }
        return CreateTableSQL.toString();
    }

    /**
     * Description:
     *    将SQLSelectItem 字段写到schema table中
     * @param  selectItemList selectItemList
     * @param CreateTableSQL StringBuffer 写入文件的内容
     * @param writer Filewriter
     */
    public void selectItemListToFile(List<SQLSelectItem> selectItemList,StringBuffer CreateTableSQL,FileWriter writer) throws IOException {
        for (SQLSelectItem selectItem : selectItemList) {
            SQLExpr selectItemExpr = selectItem.getExpr();
            String columnNameOrAlias = selectItem.getAlias();
            if (selectItemExpr instanceof SQLPropertyExpr) {
                columnNameOrAlias = ((SQLPropertyExpr) selectItem.getExpr()).getName();
                if(columnNameOrAlias.equals("*")){
                    columnNameOrAlias ="type";
                }
                if (selectItem.getAlias() != null) {
                    columnNameOrAlias = selectItem.getAlias();
                }
            }else if (selectItemExpr instanceof SQLIdentifierExpr){
                columnNameOrAlias = ((SQLIdentifierExpr) selectItem.getExpr()).getName();
            }else if(selectItemExpr instanceof  SQLAggregateExpr){
                if(selectItem.getAlias() !=null){
                    columnNameOrAlias = selectItem.getAlias();
                }//不能解析带有函数的字段，如果没有别名，先用type代替
                else {
                    columnNameOrAlias ="type";
                }
            }else if (selectItemExpr instanceof  SQLMethodInvokeExpr){
                if(selectItem.getAlias() !=null){
                    columnNameOrAlias = selectItem.getAlias();
                }//不能解析带有函数的字段，如果没有别名，先用type代替
                else {
                    columnNameOrAlias ="type";
                }
            }else if(selectItemExpr instanceof SQLAllColumnExpr){
                //不能解析*，先用Type代替
                columnNameOrAlias ="type";
            }
            CreateTableSQL.append(columnNameOrAlias +" STRING"+ ","+"\n");
        }


    }


    /**
     * Description: 主外键分析
     * @param sql
     * @param filename
     * @return
     * @throws IOException
     */
    public ForeignKeys getTableColumnForeignKeys(String sql, String filename) throws IOException {

        ForeignKeys foreignKeys = new ForeignKeys();
        TableColumnLineage tableColumnLineage = new TableColumnLineage();
        ArrayList primaryAndForeignKeyList = new ArrayList<>();
        String databaseName="";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, schemaRepository.getDbType());

        stmtList.forEach(schemaRepository::resolve);

        for (SQLStatement stmt : stmtList) {
//            continue;
            if (stmt instanceof  SQLUseStatement){
                databaseName = ((SQLUseStatement) stmt).getDatabase().getSimpleName();
            }
            //根据sql语句添加schma信息
            //CreateSchemaTable(stmt, filename, databaseName);

            //得到主外键血缘分析链路
            getPrimaryAndForeignKey(stmt, tableColumnLineage, primaryAndForeignKeyList);

            //正常的血缘分析（不可以缺少，因为可能后面拼接路径时会用到）
            databaseName = putSQLStatementColumnLineage(tableColumnLineage, stmt,databaseName);
        }

        foreignKeys.setTableColumnLineage(tableColumnLineage);
        foreignKeys.setArrayList(primaryAndForeignKeyList);
        return foreignKeys;

    }

    /**
     * Description
     * 得到主外键
     *
     * @param stmt               SQL被解析后存在druid的SQLStatement结构中
     * @param tableColumnLineage 表的字段级别血缘
     * @return 一个sql语句返回一个主外键对儿列表（待修改）TODO
     */
    public void getPrimaryAndForeignKey(SQLStatement stmt, TableColumnLineage tableColumnLineage, ArrayList primaryAndForeignKeyList) {
//        ArrayList<String> primaryAndExternalStart = new ArrayList<String>();
        //只处理建表语句
        if (stmt instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement stmtCT = (SQLCreateTableStatement) stmt;
            SQLTableSource stmCTSQLTableSource = stmtCT.getTableSource();
            SQLSelect select = stmtCT.getSelect();
            //只处理select语句
            selectSQLParse(tableColumnLineage, primaryAndForeignKeyList, stmCTSQLTableSource, select);
        }
        if (stmt instanceof SQLInsertStatement) {
            SQLInsertStatement stmtIS = (SQLInsertStatement) stmt;
            SQLTableSource stmCTSQLTableSource = stmtIS.getTableSource();
            SQLSelect select = stmtIS.getQuery();
            //只处理select语句
            selectSQLParse(tableColumnLineage, primaryAndForeignKeyList, stmCTSQLTableSource, select);
        }
    }

    /**
     *
     * @param tableColumnLineage 表的血缘关系分析
     * @param primaryAndForeignKeyList 存储主外键的列表
     * @param stmCTSQLTableSource 建表语句源
     * @param select select
     */

    private void selectSQLParse(TableColumnLineage tableColumnLineage, ArrayList primaryAndForeignKeyList, SQLTableSource stmCTSQLTableSource, SQLSelect select) {
        if (select != null) {
            //得到查询主体部分
            SQLSelectQueryBlock queryBlock = select.getQueryBlock();
            if(queryBlock!= null) {
                SQLTableSource queryBlockFrom = queryBlock.getFrom();
                //分情况对不同query进行分析
                parseSQLTableSource(queryBlockFrom, tableColumnLineage, stmCTSQLTableSource, primaryAndForeignKeyList); } }
    }

    /**
     * 根据selcet下查询语句queryBlockForm的TableSource类型，选择不同的主外键解析模式
     *
     * @param queryBlockFrom 是有join关系的query的Form部分
     */

    private void parseSQLTableSource(SQLTableSource queryBlockFrom, TableColumnLineage tableColumnLineage, SQLTableSource stmCTSQLTableSource, ArrayList primaryAndForeignKeyLists) {

        // 模式一：SQLJoinTableSource，这里存在直接join关系
        if (queryBlockFrom instanceof SQLJoinTableSource) {
            SQLJoinTableSource queryBlockFromJoin = (SQLJoinTableSource) queryBlockFrom;

            SQLExpr conditionSQLExpr = queryBlockFromJoin.getCondition();
            SQLTableSource primaryKeySQLTableSource = queryBlockFromJoin.getLeft();
            SQLTableSource foreignKeySQLTableSource = queryBlockFromJoin.getRight();
            String basename ="";
            //这里仍需要考虑，看是不是连接操作的condition一定是SQLBinaryOpExpr
            if (conditionSQLExpr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr conditionSQLBinaryOpExpr = (SQLBinaryOpExpr) conditionSQLExpr;

                //得到主键和外键的expression
                SQLExpr primaryKeySQLExpr = conditionSQLBinaryOpExpr.getLeft();
                SQLExpr foreignKeySQLExpr = conditionSQLBinaryOpExpr.getRight();

                //分析主外键，SQLExpr实际可能为SQLPropertyExpr（一般为查询字段列表）或者SQLIdentifierExpr（一般表示表名或别名）
                parsePrimaryAndForeignKey(tableColumnLineage, stmCTSQLTableSource, basename, primaryKeySQLExpr, foreignKeySQLExpr,primaryAndForeignKeyLists);
            }

//            primaryAndForeignKeyLists.add(primaryKeyAndForeignKeyList);

            //继续分析from的left语句和right语句，看是否包含子语句（递归）
            parseSQLTableSource(primaryKeySQLTableSource,tableColumnLineage, stmCTSQLTableSource,primaryAndForeignKeyLists);
            parseSQLTableSource(foreignKeySQLTableSource,tableColumnLineage, stmCTSQLTableSource,primaryAndForeignKeyLists);

        }

        // 模式二：SQLSubqueryTableSource
        else if (queryBlockFrom instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource leftSQLSubqueryTabSource = (SQLSubqueryTableSource) queryBlockFrom;
            SQLSelect select = leftSQLSubqueryTabSource.getSelect();
            //只处理select语句
            selectSQLParse(tableColumnLineage, primaryAndForeignKeyLists, stmCTSQLTableSource, select);
        }

        //模式三：SQLExprTableSource
        else if (queryBlockFrom instanceof SQLExprTableSource) {
            return;
        }

        //模式四：SQLUnionQueryTableSource
        else if (queryBlockFrom instanceof SQLUnionQueryTableSource) {
            return;
        }
    }


    /**
     *
     * @param tableColumnLineage
     * @param stmCTSQLTableSource
     * @param basename
     * @param primaryKeySQLExpr
     * @param foreignKeySQLExpr
     * @param primaryAndForeignKeyLists
     * 分析join类型，可能是普通的join关系，可能是包含cast函数的join关系，可能是包含and情况（1个join下对应多个主外键）
     */
    private void parsePrimaryAndForeignKey(TableColumnLineage tableColumnLineage, SQLTableSource stmCTSQLTableSource, String basename, SQLExpr primaryKeySQLExpr, SQLExpr foreignKeySQLExpr, ArrayList primaryAndForeignKeyLists) {

        //model 1：这里主要是存在and关系时会有多对儿主外键
        if (primaryKeySQLExpr instanceof SQLBinaryOpExpr && foreignKeySQLExpr instanceof SQLBinaryOpExpr) {
            //TOdo
            SQLBinaryOpExpr conditionPrimarySQLBinaryOpExpr = (SQLBinaryOpExpr) primaryKeySQLExpr;
            SQLExpr primaryKeyExpr = conditionPrimarySQLBinaryOpExpr.getLeft();
            SQLExpr foreignKeyExpr = conditionPrimarySQLBinaryOpExpr.getRight();
            parsePrimaryAndForeignKey(tableColumnLineage, stmCTSQLTableSource, basename, primaryKeyExpr, foreignKeyExpr, primaryAndForeignKeyLists);
            SQLBinaryOpExpr conditionForeignSQLBinaryOpExpr = (SQLBinaryOpExpr) foreignKeySQLExpr;
            SQLExpr primaryKeyExprRight = conditionForeignSQLBinaryOpExpr.getLeft();
            SQLExpr foreignKeyExperRight = conditionForeignSQLBinaryOpExpr.getRight();
            parsePrimaryAndForeignKey(tableColumnLineage, stmCTSQLTableSource, basename, primaryKeyExprRight, foreignKeyExperRight, primaryAndForeignKeyLists);

        }

        // model 2：正常的join关系的解析模式
        if (foreignKeySQLExpr instanceof SQLPropertyExpr && primaryKeySQLExpr instanceof SQLPropertyExpr) {

            String[] primaryKeyAndForeignKeyList = new String[2];
            if (primaryKeySQLExpr instanceof SQLPropertyExpr) {
                SQLPropertyExpr primaryKeySQLPropertyExpr = (SQLPropertyExpr) primaryKeySQLExpr;

                //得到主键字段（包含表名）的表名，并转化为string
                SQLExpr tableNameOfPrimaryKeyKeySQLPropertyExpr = primaryKeySQLPropertyExpr.getOwner();
                String tableNameOfPrimaryKey = tableNameOfPrimaryKeyKeySQLPropertyExpr.toString();

                //得到主键字段对应的表来源，并转换为string
                SQLTableSource primaryKeyTableSource = primaryKeySQLPropertyExpr.getResolvedTableSource();
                String primaryKeySelectSQL = primaryKeyTableSource.toString();

                //构建出主键对应的建表语句，这里是不是其实可以不写create这一步呢？直接从select开始
                String primaryKeySQL = "CREATE TABLE IF NOT EXISTS " + stmCTSQLTableSource.toString() + tableNameOfPrimaryKey + " AS " + "\n" + " SELECT " + primaryKeySQLPropertyExpr.toString() + "\n" + " FROM " + primaryKeySelectSQL + " " + tableNameOfPrimaryKey;

                List<SQLStatement> stmtList = SQLUtils.parseStatements(primaryKeySQL, schemaRepository.getDbType());
                stmtList.forEach(schemaRepository::resolve);
                for (SQLStatement stmt : stmtList) {
                    putSQLStatementColumnLineage(tableColumnLineage, stmt, basename);
                }

                primaryKeyAndForeignKeyList[0] = stmCTSQLTableSource.toString() + tableNameOfPrimaryKey;
            }

            // 分析外键
            if (foreignKeySQLExpr instanceof SQLPropertyExpr) {
                SQLPropertyExpr foreignKeySQLPropertyExpr = (SQLPropertyExpr) foreignKeySQLExpr;

                SQLExpr tableNameOfForeignKeySQLPropertyExpr = foreignKeySQLPropertyExpr.getOwner();
                String tableNameOfForeignKey = tableNameOfForeignKeySQLPropertyExpr.toString();

                SQLTableSource foreignKeyTableSource = foreignKeySQLPropertyExpr.getResolvedTableSource();
                String foreignKeySelectSQL = foreignKeyTableSource.toString();

                String foreignKeySQL = "CREATE TABLE IF NOT EXISTS " + stmCTSQLTableSource.toString() + tableNameOfForeignKey + " AS " + "\n" + " SELECT " + foreignKeySQLPropertyExpr.toString() + "\n" + " FROM " + foreignKeySelectSQL + " " + tableNameOfForeignKey;

                List<SQLStatement> stmtList = SQLUtils.parseStatements(foreignKeySQL, schemaRepository.getDbType());
                stmtList.forEach(schemaRepository::resolve);
                for (SQLStatement stmt : stmtList) {
                    putSQLStatementColumnLineage(tableColumnLineage, stmt, basename);
                }

                primaryKeyAndForeignKeyList[1] = stmCTSQLTableSource.toString() + tableNameOfForeignKey;
            }
            primaryAndForeignKeyLists.add(primaryKeyAndForeignKeyList);
        }

        //model 3：主要针对join里包含cast函数的情况
        if (foreignKeySQLExpr instanceof SQLCastExpr && primaryKeySQLExpr instanceof SQLCastExpr) {

            SQLCastExpr conditionPrimarySQLBinaryOpExpr = (SQLCastExpr) primaryKeySQLExpr;
            SQLExpr primaryKeyExpr = conditionPrimarySQLBinaryOpExpr.getExpr();

            SQLCastExpr conditionForeignSQLBinaryOpExpr = (SQLCastExpr) foreignKeySQLExpr;
            SQLExpr foreignKeyExpr = conditionForeignSQLBinaryOpExpr.getExpr();

            parsePrimaryAndForeignKey(tableColumnLineage, stmCTSQLTableSource, basename, primaryKeyExpr, foreignKeyExpr, primaryAndForeignKeyLists);

        }
    }

    /**
     * SQLUnionQuery类型得到SelectItemList
     * @param sqlSelectQuery
     * @param selectItemList
     * @return
     */
    public List<SQLSelectItem> GetSQLUnionQuerySelectItemList(SQLSelectQuery sqlSelectQuery,List<SQLSelectItem> selectItemList){
        if(sqlSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;
            if (sqlUnionQuery.getLeft() instanceof SQLUnionQuery){
                selectItemList =GetSQLUnionQuerySelectItemList(sqlUnionQuery.getLeft(),selectItemList);

            }

            if (sqlUnionQuery.getRight() instanceof SQLUnionQuery) {
                selectItemList =GetSQLUnionQuerySelectItemList(sqlUnionQuery.getRight(),selectItemList);

            }

            if (sqlUnionQuery.getLeft() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock sqlLeftSelectQuery = (SQLSelectQueryBlock) sqlUnionQuery.getLeft();
                List<SQLSelectItem> leftselectItemList = sqlLeftSelectQuery.getSelectList();
                selectItemList.addAll(leftselectItemList);
            }

            if (sqlUnionQuery.getRight() instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock sqlRightSelectQuery = (SQLSelectQueryBlock) sqlUnionQuery.getRight();
                List<SQLSelectItem> rightselectItemList = sqlRightSelectQuery.getSelectList();
                selectItemList.addAll(rightselectItemList);
            }
        } else if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            selectItemList =sqlSelectQueryBlock .getSelectList();
            if (selectItemList.get(0).getExpr() instanceof  SQLAllColumnExpr) {
                if (sqlSelectQueryBlock .getFrom() instanceof SQLExprTableSource) {
                    SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlSelectQueryBlock .getFrom();
                    if (sqlExprTableSource.getSchemaObject() != null) {

                    }
                }
            }
        }

        return selectItemList;
    }

    public List<SQLColumnDefinition> GetTableElementsList(ArrayList TableList){
        List<SQLColumnDefinition> sqlColumnDefinitions = Lists.newArrayList();
        for(Object column:TableList){
            if (column instanceof SQLColumnDefinition){
                SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) column;
                sqlColumnDefinitions.add(sqlColumnDefinition);
            }
        }
        return sqlColumnDefinitions;

    }

    public String GetTableDatabaseName(SQLExpr sqlExpr){
        List<String> name = Lists.newArrayList();
        String databaseName =" ";
        String tableName=" ";

        if (sqlExpr instanceof SQLPropertyExpr){
            tableName =((SQLPropertyExpr) sqlExpr).getName();
            databaseName =((SQLPropertyExpr) sqlExpr).getOwnernName();
        } else if (sqlExpr instanceof SQLIdentifierExpr){
            tableName=((SQLIdentifierExpr) sqlExpr).getName();
            databaseName = "tmp";
        }

        name.add(databaseName);
        name.add(tableName);

        return tableName +"\t"+databaseName;
    }


}


