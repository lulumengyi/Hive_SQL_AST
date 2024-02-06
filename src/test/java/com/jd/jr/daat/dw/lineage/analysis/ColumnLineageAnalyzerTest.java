package com.jd.jr.daat.dw.lineage.analysis;

import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.domains.lineage.column.ColumnLineageUtils;
import com.jd.jr.daat.dw.lineage.domains.lineage.table.TableColumnLineage;
import com.jd.jr.daat.dw.lineage.utils.SQLExtractor;
import com.jd.jr.daat.dw.lineage.utils.SchemaLoader;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ColumnLineageAnalyzerTest {
    private SchemaLoader schemaLoader;
    private SQLExtractor sqlExtractor;
    private SchemaRepository schemaRepository;
    private ColumnLineageAnalyzer analyzer;

    private String logsFilePath;
    private List<List<String>> sqlsEAPF;
    private String sqlLineageDemo01;
    private List<String> sqlsLineageDemo01Tables;
    private String FileTableSchemaPath;
    private List<List<String>> Allpath = new ArrayList<List<String>>();

    private List<String> results;


    @BeforeEach
    void init() throws IOException {
        schemaLoader = new SchemaLoader();
        sqlExtractor = new SQLExtractor();

        // 获取当前编译好类的路径
        String resourcesPath = getClass().getClassLoader().getResource("").getPath();

        logsFilePath = resourcesPath + "/logs";

//        sqlsEAPF = sqlExtractor.extractPathLogSQLs(logsFilePath,);

        FileTableSchemaPath = resourcesPath + "/tables/demo-01-table";

        sqlLineageDemo01 = FileUtils.readFileToString(new File(resourcesPath +
                "SQLs/demo-01"), "UTF-8");
        sqlsLineageDemo01Tables = Lists.newArrayList(FileUtils.readFileToString(new File(resourcesPath +
                "tables/demo-01-table"), "UTF-8").split(";"));
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);
        analyzer = new ColumnLineageAnalyzer(schemaRepository);
    }

    List<List<String>> testGetColumnsLineage() throws IOException {

        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);

        analyzer = new ColumnLineageAnalyzer(schemaRepository);

        TableColumnLineage tableColumnLineage = analyzer.getTableColumnLineage(sqlLineageDemo01, FileTableSchemaPath);

        tableColumnLineage.getColumnLineages().values().forEach(rootColumnNode -> {
            results = ColumnLineageUtils.getFirstPathString(rootColumnNode);
            Allpath.add(results);
        });
        return Allpath;

    }


    @Test
    /**
     * Description:
     *   合并头尾连接的path
     *
     */
    public void pathMerge() throws IOException {
        List<List<String>> Allpath = testGetColumnsLineage();
        List<String> path = new ArrayList<>();
        List<List<String>> mergedPath ;
        for (List<String> path1 : Allpath) {
            path.add(path1.get(0));//得到所有path集合
        }
        mergedPath = mergePath(Allpath, Allpath, path);
        while(!mergedPath.isEmpty()){
            mergedPath= mergePath(Allpath,mergedPath,path) ;
        }
        System.out.println("--------------------");
        path.stream().forEach(System.out::println);

        //循环遍历 判断当前path1的尾部是否是path2的头部，如果是连接，并删除对应的path，
        //一个问题：合并后的path可能还要和其他的path合并

    }


    public List<List<String>> mergePath(List<List<String>> Allpath1, List<List<String>> Allpath2, List<String> path) {
        List<String> mergePathItem;
        List<List<String>> mergedPath = new ArrayList<>();
        for (List<String> path1 : Allpath1) {
            String end_1 = path1.get(2);
            String path_1 = path1.get(0);
            String start_1 = path1.get(1);
            for (List<String> path2 : Allpath2) {
                String start_2 = path2.get(1);
                String path_2 = path2.get(0);
                String end_2 = path2.get(2);
                if (start_2.equals(end_1)) {
                    String mergepath = path_1.substring(0, path_1.length() - 5) +
                            path_2.substring(10, path_2.length());
                    mergePathItem = stringToList(mergepath, start_1, end_2);
                    mergepath = pathAddMergePath(mergepath, path);
                    if (!path.contains(mergepath)) {
                        path.add(mergepath);
                    }
                    mergedPath.add(mergePathItem);
                    path.remove(path_1);

                }
            }
        }

        return mergedPath;

    }


    public String pathAddMergePath(String mergedpath ,List<String> pathList){
        String addMergePath="" ;
        Iterator<String> it = pathList.iterator();
        while(it.hasNext()){
            String path = it.next();
            String mergedPathStart = pathGetStart(mergedpath);
            String pathStart =pathGetStart(path);
            if(mergedPathStart.equals(pathStart) && mergedpath.length()>path.length()){
                addMergePath=mergedpath;
                it.remove();
                break;
            }else {

                addMergePath=path;
            }
        }
        return addMergePath;
    }
    public String pathGetStart(String path){
        String table_name="" ;
        String column_name="" ;
        String table_name_str = "(?<=table: )[A-Za-z0-9_]{1,50}";
        String column_name_str ="(?<=, column: )[a-z_]{1,20}";
        String database_name_str = "(?<=, database: )[a-z_]{1,20}";

        Pattern table_name_pattern = Pattern.compile(table_name_str);
        Pattern column_name_pattern = Pattern.compile(column_name_str);
        Matcher table_matcher = table_name_pattern.matcher(path);
        Matcher column_matcher = column_name_pattern.matcher(path);
        if(table_matcher.find()){
            table_name = table_matcher.group();
        }
        if(column_matcher.find()){
            column_name = column_matcher.group();
        }

        return table_name+"."+column_name;
    }

    public List<String> stringToList(String path,String start,String end){
        List<String> pathItem =new ArrayList<>();
        pathItem.add(path);
        pathItem.add(start);
        pathItem.add(end);
        return pathItem;
    }

}
