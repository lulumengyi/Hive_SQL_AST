package com.jd.jr.daat.dw.lineage.analysis;

import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.domains.basic.ForeignKeys;
import com.jd.jr.daat.dw.lineage.domains.lineage.column.ColumnLineageUtils;
import com.jd.jr.daat.dw.lineage.domains.lineage.table.TableColumnLineage;
import com.jd.jr.daat.dw.lineage.utils.SQLExtractor;
import com.jd.jr.daat.dw.lineage.utils.SchemaLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GetPrimaryAndForeignKeyTest {
    private SchemaLoader schemaLoader;
    private SQLExtractor sqlExtractor;
    private SchemaRepository schemaRepository;
    private ColumnLineageAnalyzer analyzer;

    private String logsFilePath;
    private List<List<String>> sqlsEAPF;
    private String sqlLineageDemo01;
    private List<String> sqlsLineageDemo01Tables;
    private String FileTableSchemaPath;
    private String FilePrimaryForeignPath;

    private List<List<String>> Allpath = new ArrayList<List<String>>();

    private List<String> results;


    @BeforeEach
    void init() throws IOException {
        schemaLoader = new SchemaLoader();
        sqlExtractor = new SQLExtractor();

        // 获取当前编译好类的路径
        String resourcesPath = getClass().getClassLoader().getResource("").getPath();

        logsFilePath = resourcesPath + "/logs";

        //sqlsEAPF = sqlExtractor.extractPathLogSQLs(logsFilePath);

        FileTableSchemaPath = resourcesPath + "schemaTable.sql";
        FilePrimaryForeignPath =resourcesPath +"LineagePrimaryAndForeign_2.csv";

        sqlLineageDemo01 = FileUtils.readFileToString(new File(resourcesPath +
                "SQLs/ParsedSQL"), "UTF-8");
        sqlsLineageDemo01Tables = Lists.newArrayList(FileUtils.readFileToString(new File(resourcesPath +
                "tables/demo-01-table"), "UTF-8").split(";"));
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);
        analyzer = new ColumnLineageAnalyzer(schemaRepository);

    }


    @Test
    /**
     * Description:
     *   合并头尾连接的path，得到主外键结果
     *
     */
    public void testGetPrimaryAndForeignKey() throws IOException {
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);

        analyzer = new ColumnLineageAnalyzer(schemaRepository);

        // ForeignKeys是一个存储结构，存储了血缘分析结果tableColumnLineage（原始sql的血缘分析和用主外键构造出sql的血缘分析结果）和最终链接到物理表的主外键对列表
//        ForeignKeys storeStruct = analyzer.getTableColumnLineage(sqlLineageDemo01, FileTableSchemaPath);

        ForeignKeys storeStruct = analyzer.getTableColumnForeignKeys(sqlLineageDemo01, FileTableSchemaPath);

        //primaryAndforeignKeyList是最主外键对列表；tableColumnLineage是血缘分析结果
        ArrayList primaryAndforeignKeyList = storeStruct.getArrayList();
        TableColumnLineage tableColumnLineage = storeStruct.getTableColumnLineage();
        System.out.println("GetColumnLineage and primaryAndforeignKeyList");

        //连接血缘分析路径，使最终链接到物理表的字段
        List<String> path = getPathMerged(tableColumnLineage);

        System.out.println("--------------------");
        int count =0;
        File file=new File(FilePrimaryForeignPath);
        FileWriter writer = new FileWriter(file, true);
        CSVFormat csvFormat = CSVFormat.newFormat('\t');
        CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

        //多条sql语句一共有多少的join关系，这里就循环多少次
        for(Object primaryAndFreignKey:primaryAndforeignKeyList){

            String[] primaryAndFreignKeyList = (String[])primaryAndFreignKey;
            String primaryKey="";
            String foreignKey="";

            //得到sql的join关系对应的主外键的初始字段
            try{
                primaryKey = primaryAndFreignKeyList[0].split("\\.")[1];
                foreignKey = primaryAndFreignKeyList[1].split("\\.")[1];
            }catch (ArrayIndexOutOfBoundsException e){
                count++;
                System.out.println(count);
                continue;
            }

            //primaryAndForeignRealList用于接收血缘分析后对应物理表的主外键对儿
            String[] primaryAndForeignRealList = new String[2];
            for (String pathPer : path) {

                //根据血缘分析结果得到初始临时表字段名和最终物理表对应的字段名
                String[] tableNameS = pathPer.split("->");
                String startTableName = tableNameS[1].split(",")[2].substring(8);
                String realColunmnName = tableNameS[tableNameS.length - 2];

                //得到临时表join关系对应字段血缘关系分析后到最终物理表的字段，也就是主外键
                if (primaryKey.equals(startTableName) ) {
                    primaryAndForeignRealList[0] = realColunmnName;
                }
                if (foreignKey.equals(startTableName)) {
                    primaryAndForeignRealList[1] = realColunmnName;
                }
            }

            // 去除掉未映射到最终物理表的和主外键相同的，然后输出
            String notRealLabel = primaryAndForeignRealList[1].split(",")[1].substring(1,5);
            if (!"STOP".equals(notRealLabel) && !primaryAndForeignRealList[0].equals(primaryAndForeignRealList[1])){
                String concatPrimaryForeignPath = primaryAndForeignRealList[0]+primaryAndForeignRealList[1];
                String[] writeLine = pathGetStart(concatPrimaryForeignPath ).split("\t");
                csvPrinter.printRecord(Arrays.asList(writeLine));
                csvPrinter.flush();
                //System.out.println(primaryAndForeignRealList[0] + " " + primaryAndForeignRealList[1]);
            }

        }
        csvPrinter.close();
        writer.close();

    }

    /**
     * Description: 对链路进行合并，得到最后的头尾节点
     * 思想：
     * @param tableColumnLineage :所有血缘关系的的链路
     * @return
     */

    private List<String> getPathMerged(TableColumnLineage tableColumnLineage) throws IOException {
        //ALLpath存放的所有血缘关系的直接链路（未合并的）
        tableColumnLineage.getColumnLineages().values().forEach(rootColumnNode -> {
            results = ColumnLineageUtils.getFirstPathString(rootColumnNode);
            if(results!=null){
                Allpath.add(results);}
        });
        //path存放最后合并好的链路
        List<String> path = new ArrayList<>();
        //mergedPath存放每次合并的链路
        List<List<String>> mergedPath ;

        for (List<String> path1 : Allpath) {
            path.add(path1.get(0));//得到所有path集合
        }

        mergedPath = mergePath(Allpath, Allpath, path);
        int i=0;
        System.out.println("mergedpath "+ i);
        while(!mergedPath.isEmpty()){
            i+=1;
            mergedPath = mergePath(Allpath,mergedPath,path);
            System.out.println("mergedpath "+ i);
        }

        System.out.println("--------");
        System.out.println(path.size());

        return path;
    }

    /**
     * Description: 得到合并的路径
     * @param Allpath1 存放所有血缘关系的直接路径
     * @param Allpath2 存放每次合并后的路径
     * @param path 存放最后合并好的路径，也就是最后打印的路径
     * @return 返回每次合并的路径
     */

    /**
     * Description: 得到合并的路径
     * @param Allpath1 存放所有血缘关系的直接路径
     * @param Allpath2 存放每次合并后的路径
     * @param path 存放最后合并好的路径，也就是最后打印的路径
     * @return 返回每次合并的路径
     */
    public List<List<String>> mergePath(List<List<String>> Allpath1, List<List<String>> Allpath2, List<String> path) throws IOException {
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
                    String mergedPathStart = pathGetStart(mergepath).split("\t")[0] +pathGetStart(mergepath).split("\t")[1]+pathGetStart(mergepath).split("\t")[2];
                    String mergedPathEnd = pathGetStart(mergepath).split("\t")[3] +pathGetStart(mergepath).split("\t")[4]+pathGetStart(mergepath).split("\t")[5];
                    if (!path.contains(mergepath) && !mergedPathStart.equals(mergedPathEnd) && !start_1.equals(end_2)) {
                        path.add(mergepath);
                        mergePathItem = stringToList(mergepath, start_1, end_2);
                        mergedPath.add(mergePathItem);
                    }
                    path.remove(path_1);
                }
            }
        }

        return mergedPath;

    }

    /**
     * Description: 得到一条路径的头尾
     * @param path  输入路径
     * @return 返回 路径的头尾 start databasename tablename columnname end databasename tablename columnname
     */
    public String pathGetStart(String path){
        String table_name=" " ;
        String column_name=" " ;
        String database_name =" ";
        String end_table_name= " ";
        String end_column_name =" ";
        String end_database_name=" ";
        String table_name_str = "(?<=, table: )[A-Za-z0-9_]{1,60}";
        String column_name_str ="(?<=, column: )[A-Za-z()*0-9_-]{1,60}";
        String database_name_str ="(?<=, database: )[A-Za-z_]{1,20}";
        String end_column_name_str ="(?<=R, type: )UNSUPPORTED";
        //String end_database_name_str ="(?<=, database: )[a-z_]{1,20}";
        Pattern table_name_pattern = Pattern.compile(table_name_str);
        Pattern column_name_pattern = Pattern.compile(column_name_str);
        Pattern end_column_name_pattern = Pattern.compile(end_column_name_str);
        Pattern database_name_pattern = Pattern.compile(database_name_str);

        Matcher table_matcher = table_name_pattern.matcher(path);
        Matcher column_matcher = column_name_pattern.matcher(path);
        Matcher end_column_matcher = end_column_name_pattern.matcher(path);
        Matcher database_matcher = database_name_pattern.matcher(path);

        if(table_matcher.find()){
            table_name = table_matcher.group();
        }
        while(table_matcher.find()){
            end_table_name = table_matcher.group();
        }
        if(column_matcher.find()){
            column_name = column_matcher.group();
        }
        while (column_matcher.find()){
            end_column_name = column_matcher.group();
        }
        if(database_matcher.find()){
            database_name = database_matcher.group();
        }
        while (database_matcher.find()){
            end_database_name = database_matcher.group();
        }
        if(end_column_matcher.find()){
            end_column_name = end_column_matcher.group();
        }
        return database_name+"\t"+table_name+"\t"+column_name +"\t"+end_database_name+"\t"+end_table_name+"\t"+end_column_name;
    }

    /**
     * Description : 将路径头和尾合并成列表
     * @param path
     * @param start
     * @param end
     * @return
     */
    public List<String> stringToList(String path,String start,String end){
        List<String> pathItem =new ArrayList<>();
        pathItem.add(path);
        pathItem.add(start);
        pathItem.add(end);
        return pathItem;
    }




}
