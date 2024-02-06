package com.jd.jr.daat.dw.lineage.analysis;

import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
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

public class ColunmnTest {
    private SchemaLoader schemaLoader;
    private SQLExtractor sqlExtractor;
    private SchemaRepository schemaRepository;
    private ColumnLineageAnalyzer analyzer;
    private GetColumnsLineage getColumnsLineage;
    private String FileLineagePath;
    private String FileMergePath;

    private String logsFilePath;
    private List<List<String>> sqlsEAPF;
    private String sqlLineageDemo01;
    private List<String> sqlsLineageDemo01Tables;
    private String FileTableSchemaPath;
    private String FileLineageHeadTail;
    private List<List<String>> Allpath = new ArrayList<List<String>>();

    private List<String> results;
    @BeforeEach
    void init() throws IOException {
        schemaLoader = new SchemaLoader();
        sqlExtractor = new SQLExtractor();
        getColumnsLineage =new GetColumnsLineage();
        // 获取当前编译好类的路径
        String resourcesPath = getClass().getClassLoader().getResource("").getPath();

        logsFilePath = resourcesPath + "/logs";

        FileTableSchemaPath = resourcesPath + "/tables/demo-01-table";
        FileLineagePath = resourcesPath +"path.sql";
        FileMergePath = resourcesPath +"merge.sql";
        FileLineageHeadTail = resourcesPath +"LineageHeadTail_ns_jrdw_internal_demoo.csv";

        sqlLineageDemo01 = FileUtils.readFileToString(new File(resourcesPath +
                "process_unparsed/GetParsed.sql"), "UTF-8");
        sqlsLineageDemo01Tables = Lists.newArrayList(FileUtils.readFileToString(new File(resourcesPath +
                "tables/demo-01-table"), "UTF-8").split(";"));
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);
        analyzer = new ColumnLineageAnalyzer(schemaRepository);

    }
    /**
     * Description: 对链路进行合并，得到最后的头尾节点
     * 思想：
     * @param
     * @return
     */
    @Test
    void testPathMerge() throws IOException {
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);
        analyzer = new ColumnLineageAnalyzer(schemaRepository);
        List<List<String>> Allpath =getColumnsLineage.getAllpath(analyzer,sqlLineageDemo01,FileTableSchemaPath);
        List<String> path = new ArrayList<>();
        List<List<String>> mergedPath ;

        System.out.println(Allpath.size());
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
        GetLineageHeadAndTail(path);

        //path.stream().forEach(System.out::println);
    }

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

        File mergefile=new File(FileMergePath);
        FileWriter mergewriter = new FileWriter(mergefile, true);

       // System.out.println("enter");
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
//                        mergewriter.write(mergepath+"\n");
//                        mergewriter.write(start_1+"\n"+end_2);
//                        mergewriter.write("\n"+"\n");
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

    /**
     * Description : 得到路径的头尾，也即是最后的结果
     * @param pathList 所有合并好的路径
     * @throws IOException
     */
    public void GetLineageHeadAndTail(List<String> pathList) throws IOException {
        File file=new File(FileLineageHeadTail);
        FileWriter writer = new FileWriter(file, true);
        CSVFormat csvFormat = CSVFormat.newFormat('\t');
        CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

        for(String path:pathList){
            //只包含头尾都是物理表的路径
            if(!pathGetStart(path).split("\t")[0].equals("TMP") && !pathGetStart(path).split("\t")[0].equals("tmp")&&!pathGetStart(path).split("\t")[3].equals("tmp")&&!pathGetStart(path).split("\t")[3].equals("TMP")){
                System.out.println(pathGetStart(path));
                String[] writeLine=pathGetStart(path).split("\t");
                csvPrinter.printRecord(Arrays.asList(writeLine));
                csvPrinter.flush();

            }

        }

        csvPrinter.close();
        writer.close();
    }
}
