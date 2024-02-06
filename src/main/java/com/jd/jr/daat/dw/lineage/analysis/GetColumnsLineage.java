package com.jd.jr.daat.dw.lineage.analysis;

import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.domains.lineage.column.ColumnLineageUtils;
import com.jd.jr.daat.dw.lineage.domains.lineage.table.TableColumnLineage;
import com.jd.jr.daat.dw.lineage.utils.ProcessUnparsedSQL;
import com.jd.jr.daat.dw.lineage.utils.SQLExtractor;
import com.jd.jr.daat.dw.lineage.utils.SchemaLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetColumnsLineage {

    private List<List<String>> Allpath = new ArrayList<List<String>>();

    private List<String> results;

    public static void main(String[] args) throws IOException {
        SchemaLoader schemaLoader = new SchemaLoader();
        GetColumnsLineage getColumnsLineage =new GetColumnsLineage();

        // 获取当前编译好类的路径
        //resourcePath
        String resourcePath = args[0];
        //tablePath
        String tableSchemaPath = args[1];
        //日志文件
        String logPath = args[2];
        //最后的输出文件，链路只包含头尾
        String LineageHeadTailPath = args[3];
        //文件后缀命名
        String LineageName =args[4];

        String sqlExtractorPath= resourcePath+"/process_sql/"+LineageName +"_sqlExtractor.sql";

        System.out.println("=====extract sql from logs================");
        getColumnsLineage.getSQLExtractorPath(logPath,sqlExtractorPath);
        System.out.println("======extracted !!================");

        System.out.println("=========getParsedSQL=============");
        String parsedSQLPath = getColumnsLineage.getParsedSQLPath(resourcePath,sqlExtractorPath,LineageName);
        System.out.println("=========done!!===================");

        System.out.println("===========getLineage================");
        String sqlLineage = FileUtils.readFileToString(new File(parsedSQLPath), "UTF-8");
        List<String> sqlsLineageTables = Lists.newArrayList(FileUtils.readFileToString(new File(tableSchemaPath), "UTF-8").split(";"));
        SchemaRepository schemaRepository = schemaLoader.load(sqlsLineageTables, JdbcConstants.HIVE);
        ColumnLineageAnalyzer  analyzer = new ColumnLineageAnalyzer(schemaRepository);
        getColumnsLineage.GetColumnsLineage(analyzer,sqlLineage,tableSchemaPath, LineageHeadTailPath);
        System.out.println("===========done!!==============");
    }



    /**
     * 从SQLExtractor抽取sql
     * @param logsPath
     * @param writeSqlFileName
     * @return
     * @throws IOException
     */
    public String getSQLExtractorPath(String logsPath,String writeSqlFileName) throws IOException {
        SQLExtractor sqlExtractor= new SQLExtractor();
        BufferedWriter sqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(writeSqlFileName,true),"UTF-8"));

        sqlExtractor.extractPathLogSQLs(logsPath,sqlWriter);
        return writeSqlFileName;

    }

    public String getParsedSQLPath(String resourcesPath,String sqlFile,String Name) throws IOException {
        //从SQLExtractor提取出来的sql，进行过滤得到不能解析的SQL,送到SQLprocessUnparsed处理，处理后再用。
        ProcessUnparsedSQL processUnparsedSQL =new ProcessUnparsedSQL();
        String getUnparsedSqlPath = resourcesPath + "/process_sql/"+Name+"_GetUnParsed.sql";
        String getParsedSqlPath = resourcesPath + "/sql/"+Name+"_GetParsed.sql";
        processUnparsedSQL.filterSQL(resourcesPath,sqlFile,getUnparsedSqlPath,getParsedSqlPath);
        //把不能解析的sql进行处理
        String getProcessedSQLPath = resourcesPath +"/process_sql/"+Name+"_ProcessedSQL.sql";
        String getUnprocessedSQLPath = resourcesPath +"/process_sql/"+Name+"_UnprocessedSQL.sql";
        String processedSQL = processUnparsedSQL.processUnParsedSQL(resourcesPath,getUnparsedSqlPath,getProcessedSQLPath,getUnprocessedSQLPath );
        //将处理过的sql得到可以解析的
        processUnparsedSQL.filterSQL(resourcesPath,processedSQL,getUnparsedSqlPath,getParsedSqlPath);
        return getParsedSqlPath;

    }

    void GetColumnsLineage(ColumnLineageAnalyzer analyzer,String sqlLineageDemo01,String FileTableSchemaPath, String FileLineageHeadTail) throws IOException {
        List<List<String>> Allpath =getAllpath(analyzer,sqlLineageDemo01,FileTableSchemaPath);
        List<String> path = new ArrayList<>();
        List<List<String>> mergedPath ;

        //System.out.println(Allpath.size());
        for (List<String> path1 : Allpath) {
            path.add(path1.get(0));//得到所有path集合
        }

        mergedPath = mergePath(Allpath, Allpath, path);
        int i=0;
        //System.out.println("mergedpath "+ i);
        while(!mergedPath.isEmpty()){
            i+=1;
            mergedPath = mergePath(Allpath,mergedPath,path);
            //System.out.println("mergedpath "+ i);
        }

        //System.out.println("--------");
        //System.out.println("链路条数"+path.size());
        getLineageHeadAndTail(path, FileLineageHeadTail);
        //path.stream().forEach(System.out::println);
    }



    public List<List<String>> getAllpath(ColumnLineageAnalyzer analyzer,String sqlLineageDemo01,String FileTableSchemaPath) throws IOException {
        TableColumnLineage tableColumnLineage = analyzer.getTableColumnLineage(sqlLineageDemo01, FileTableSchemaPath);
        tableColumnLineage.getColumnLineages().values().forEach(rootColumnNode -> {
            results = ColumnLineageUtils.getFirstPathString(rootColumnNode);
            if(results!=null){
                Allpath.add(results);
            }
        });
        //System.out.println(Allpath.size());
        return Allpath;

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

    /**
     * Description : 得到路径的头尾，也即是最后的结果
     * @param pathList 所有合并好的路径
     * @throws IOException
     */
    public void getLineageHeadAndTail(List<String> pathList, String FileLineageHeadTail) throws IOException {

        File file=new File(FileLineageHeadTail);
        BufferedWriter bufferedWriter = Files.newBufferedWriter(Paths.get(FileLineageHeadTail));
        CSVFormat csvFormat = CSVFormat.newFormat('\t').withRecordSeparator('\n');
        CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, csvFormat);

        //System.out.println("------------------");
        //System.out.println(pathList.size());

        for(String path:pathList){
            //只包含头尾都是物理表的路径
            if(!pathGetStart(path).split("\t")[0].equals("TMP") && !pathGetStart(path).split("\t")[0].equals("tmp")&&!pathGetStart(path).split("\t")[3].equals("tmp")&&!pathGetStart(path).split("\t")[3].equals("TMP")){
                //System.out.println(pathGetStart(path));
                String[] writeLine=pathGetStart(path).split("\t");
                csvPrinter.printRecord(Arrays.asList(writeLine));
                csvPrinter.flush();
            }
        }

        csvPrinter.close();
        bufferedWriter.close();
    }
}
