package com.jd.jr.daat.dw.lineage.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProcessUnparsedSQL {

    /**
     * 过滤SQL
     * @param resourcesPath
     * @param sqlFile 从SQLExtractor抽出的sql文件
     * @throws IOException
     */
    public void filterSQL(String resourcesPath, String sqlFile, String getUnparsedSqlPath, String getParsedSqlPath) throws IOException {
        SchemaLoader schemaLoader = new SchemaLoader();
        SchemaRepository schemaRepository =new SchemaRepository();
        List<String> sqlLineageDemo01;
        List<String> sqlsLineageDemo01Tables;

        // 获取当前编译好类的路径
        String logsFilePath = resourcesPath + "/logs";
        String FileTableSchemaPath = resourcesPath + "/table/table";

        int count=0;
        sqlLineageDemo01 = Lists.newArrayList(FileUtils.readFileToString(new File(sqlFile), "UTF-8").split(";"));
        sqlsLineageDemo01Tables = Lists.newArrayList(FileUtils.readFileToString(new File(FileTableSchemaPath), "UTF-8").split(";"));

        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);

        BufferedWriter unparsedSqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(getUnparsedSqlPath,true),"UTF-8"));
        BufferedWriter parsedSqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(getParsedSqlPath,true),"UTF-8"));

        // newStruct是一个存储结构，存储了血缘分析结果tableColumnLineage（原始sql的血缘分析和用主外键构造出sql的血缘分析结果）和最终链接到物理表的主外键对列表

        for (String sql : sqlLineageDemo01) {
            try {
                List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, schemaRepository.getDbType());

                stmtList.forEach(schemaRepository::resolve);

                parsedSqlWriter.write(sql+";");
                continue;

            } catch (com.alibaba.druid.sql.parser.ParserException e) {
                //System.out.println(sql);
                count++;

                unparsedSqlWriter.write(sql+";");
            }
        }

        unparsedSqlWriter.flush();
        unparsedSqlWriter.close();
        parsedSqlWriter.flush();
        parsedSqlWriter.close();

    }

    /**
     * 从文件中得到未解析的sql
     * @param
     */
    public String processUnParsedSQL(String resourcesPath,String UnparsedSQLPath,String getProcessedSQLPath ,String getUnprocessedSQLPath ) throws IOException {
        // 获取当前编译好类的路径


        BufferedWriter processedWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(getProcessedSQLPath,true),"UTF-8"));
        BufferedWriter unprocessedWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(getUnprocessedSQLPath,true),"UTF-8"));

        List<String> sqlLineageDemo01 = Lists.newArrayList(FileUtils.readFileToString(new File(UnparsedSQLPath), "UTF-8").split(";"));
        //过滤掉不能解析的sql语句
        for (String sql : sqlLineageDemo01){
            String beforeSQL=sql;
            sql = processUnparsedCreateSQL(sql);
            sql = processUnparsedSplitSQL(sql);
            sql = processUnparsedSortSQL(sql);
            sql = processUnparsedGroupingSets(sql);
            sql = processUnparsedDistributeBy(sql);
            sql = processUnparsedBracketSQL(sql);
            sql = processUnparsedOver(sql);
            sql = processUnparsedOver1(sql);
            sql = processUnparseInteger(sql);
            sql = processUnparsedIndex(sql);

            //如果sql还是等于之前内容，说明该sql不符合以上能解析的情况。
            if (!sql.equals("")){
                if (beforeSQL.equals(sql)){
                    unprocessedWriter.write(sql+";"+"\n");
                } else {
                    processedWriter.write(sql+";"+"\n");
                }
            }

        }

        unprocessedWriter.flush();
        unprocessedWriter.close();
        processedWriter.flush();
        processedWriter.close();

        return getProcessedSQLPath;
    }

    /**
     * 处理split函数，把split函数去掉,留下中间的字段
     *
     */

    public String processUnparsedSplitSQL(String sql) throws IOException {
        //String sql = "split(regexp_replace(requesturldecode,'^http[s]?://m.jr.jd.com/mjractivity/',''),'-')[0] id";
        //String sql ="split(split(split(spoint2,'#')[2],'_')[1],'[*]')[1] as from_sec_page_card_name";

        final String SQL_SPLIT_PREFIX_REGEX_STR ="split\\s*\\(";
        final String SQL_SPLIT_SUFFFIX_REGEX_STR ="\\,\\s*\'*[a-zA-Z\\d\\_\\-\"|%#&；.,:*?{}()=\\[\\]\\\\]{1,30}\\s*\'*\\s*\\)\\s*\\[\\d+\\]";

        Pattern SQL_SPLIT_PREFIX_PATTERN=Pattern.compile(SQL_SPLIT_PREFIX_REGEX_STR);
        Matcher SQL_SPLIT_PREFIX_MATCHER = SQL_SPLIT_PREFIX_PATTERN.matcher(sql.toLowerCase());

        Pattern SQL_SPLIT_SUFFIX_PATTERN=Pattern.compile(SQL_SPLIT_SUFFFIX_REGEX_STR);
        String s="";
        String finalSQL="";

        while(SQL_SPLIT_PREFIX_MATCHER.find()){
            s =SQL_SPLIT_PREFIX_MATCHER.replaceAll("");
        }

        Matcher SQL_SPLIT_SUFFIX_MATCHER = SQL_SPLIT_SUFFIX_PATTERN.matcher(s);
        while(SQL_SPLIT_SUFFIX_MATCHER.find()){
            finalSQL = SQL_SPLIT_SUFFIX_MATCHER.replaceAll("");
        }

        if(!finalSQL.equals("")){
            sql =finalSQL;
        }

        return sql;
    }


    /**
     * 处理row_number 、rank、 count(1) Sum() over 语句 ,直接去掉，保留后面的别名
     */

    public String processUnparsedSortSQL(String sql){
        //String sql ="row_Number() over(distribute by item_first_cate_id,item_second_cate_id,item_third_cate_id sort by ratio desc,good_comment_ratio desc,price) rank_sku\n";
        final String SQL_SORT_REGEX_STR ="(row_number)\\s*\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(rank)\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(count)\\(1\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(sum\\(\\w+\\))\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*";

        String repalce_SQL="";
        Pattern SQL_SORT_PATTERN=Pattern.compile(SQL_SORT_REGEX_STR);
        Matcher SQL_SORT_MATCHER = SQL_SORT_PATTERN.matcher(sql.toLowerCase());

        while (SQL_SORT_MATCHER.find()){
            repalce_SQL = (SQL_SORT_MATCHER.replaceAll(""));
        }

        if (!repalce_SQL.equals("")){
            sql=repalce_SQL;
        }

        return sql;
    }

    /**
     *
     * @param sql
     * @return
     */
    public String processUnparsedBracketSQL(String sql){
        //String sql ="row_Number() over(distribute by item_first_cate_id,item_second_cate_id,item_third_cate_id sort by ratio desc,good_comment_ratio desc,price) rank_sku\n";
        final String SQL_SORT_REGEX_STR ="\\[\\d+\\]";

        String repalce_SQL="";
        Pattern SQL_SORT_PATTERN=Pattern.compile(SQL_SORT_REGEX_STR);
        Matcher SQL_SORT_MATCHER = SQL_SORT_PATTERN.matcher(sql.toLowerCase());

        while (SQL_SORT_MATCHER.find()){
            repalce_SQL = (SQL_SORT_MATCHER.replaceAll(""));
        }

        if (!repalce_SQL.equals("")){
            sql=repalce_SQL;
        }

        return sql;

    }
    /**
     * 处理含有DistributeBy
     */
    public String processUnparsedDistributeBy(String sql){

        String SQL_OVER_DISTRIBUTE_BY_REGEX_STR = "over\\s*\\(\\s*distribute by.*\\)";
        Pattern SQL_OVER_DISTRIBUTE_BY_PATTERN= Pattern.compile(SQL_OVER_DISTRIBUTE_BY_REGEX_STR);
        Matcher SQL_OVER_DISTRIBUTE_BY_MATCHER = SQL_OVER_DISTRIBUTE_BY_PATTERN.matcher(sql.toLowerCase());

        String SQL_DISTRIBUTE_BY_REGEX_STR = "distribute by\\s*\\w+";
        Pattern SQL_DISTRIBUTE_BY_PATTERN= Pattern.compile(SQL_DISTRIBUTE_BY_REGEX_STR);
        Matcher SQL_DISTRIBUTE_BY_MATCHER = SQL_DISTRIBUTE_BY_PATTERN.matcher(sql.toLowerCase());

        while (SQL_OVER_DISTRIBUTE_BY_MATCHER.find()){
//            System.out.println("ok");
//            System.out.println(SQL_DISTRIBUTE_BY_MATCHER.group());
            sql = SQL_OVER_DISTRIBUTE_BY_MATCHER.replaceAll("");
        }

        while (SQL_DISTRIBUTE_BY_MATCHER.find()){
//            System.out.println("ok");
//            System.out.println(SQL_DISTRIBUTE_BY_MATCHER.group());
            sql = SQL_DISTRIBUTE_BY_MATCHER.replaceAll("");
        }


        //System.out.println(sql);
        return sql;

    }

    /**
     * 处理不能解析的含create语句，直接把create语句删掉
     */

    public String processUnparsedCreateSQL(String sql){

        final String SQL_CREATE_REGEX_STR ="create\\s*table\\s*(if not exists)*.*";
        String repalce_SQL="";
        Pattern SQL_CREATE_PATTERN=Pattern.compile(SQL_CREATE_REGEX_STR);
        Matcher SQL_CREATE_MATCHER = SQL_CREATE_PATTERN.matcher(sql.toLowerCase());

        final String SQL_SELECT_REGEX_STR = "select\\s+";
        Pattern SQL_SELECT_PATTERN=Pattern.compile(SQL_SELECT_REGEX_STR);
        Matcher SQL_SELECT_MATCHER = SQL_SELECT_PATTERN.matcher(sql.toLowerCase());


        if (SQL_CREATE_MATCHER.find()&&!SQL_SELECT_MATCHER.find()){
            sql = repalce_SQL;
        }

        return sql;

    }

    /**
     * 处理含有grouping sets语句，找到该语句用空格代替
     * @param
     * @return
     */
    public String processUnparsedGroupingSets(String sql){

        String SQL_GROUP_REGEX_STR = "grouping sets\\s*\\n*\\([^\\(\\)]*(\\([^\\(\\)]*(\\([^\\(\\)]*\\)[^\\(\\)]*)*\\)[^\\(\\)]*)*\\)";

        Pattern SQL_GROUP_PATTERN= Pattern.compile(SQL_GROUP_REGEX_STR);
        Matcher SQL_GROUP_MATCHER = SQL_GROUP_PATTERN.matcher(sql.toLowerCase());

        while (SQL_GROUP_MATCHER.find()){

            sql = SQL_GROUP_MATCHER.replaceAll("");
        }

        return sql;

    }

    /**
     * 处理over类型的SQL
     */
    public String processUnparsedOver(String sql){

        String SQL_OVER_REGEX_STR = "over\\(.*\\)";

        Pattern SQL_OVER_PATTERN= Pattern.compile(SQL_OVER_REGEX_STR);
        Matcher SQL_OVER_MATCHER = SQL_OVER_PATTERN.matcher(sql.toLowerCase());

        while (SQL_OVER_MATCHER.find()){
            sql = SQL_OVER_MATCHER.replaceAll("");
        }
        return sql;


    }

    /**
     * 处理含换行的over类型的SQL
     */
    public String processUnparsedOver1(String sql){

        String SQL_OVER_REGEX_STR = "over\\(\\n.*\\n.*\\)";

        Pattern SQL_OVER_PATTERN= Pattern.compile(SQL_OVER_REGEX_STR);
        Matcher SQL_OVER_MATCHER = SQL_OVER_PATTERN.matcher(sql.toLowerCase());

        while (SQL_OVER_MATCHER.find()){
            sql = SQL_OVER_MATCHER.replaceAll("");
        }

        return sql;


    }

    /**
     * 处理含数字的字段
     * @param sql
     * @return
     */
    public String processUnparseInteger(String sql){
        String SQL_Integer_REGEX_STR = "as\\s(\\d+_)+";

        Pattern SQL_Integer_PATTERN= Pattern.compile(SQL_Integer_REGEX_STR);
        Matcher SQL_Integer_MATCHER = SQL_Integer_PATTERN.matcher(sql.toLowerCase());

        while (SQL_Integer_MATCHER.find()){
            sql = SQL_Integer_MATCHER.replaceAll(" ");
        }

        return sql;
    }

    /**
     * 处理包含关键字index的sql,替换成index_
     * @param sql
     * @return
     */
    public String processUnparsedIndex(String sql){
        String SQL_Index_REGEX_STR = "index(?!\\w)";

        Pattern SQL_Index_PATTERN= Pattern.compile(SQL_Index_REGEX_STR);
        Matcher SQL_Index_MATCHER = SQL_Index_PATTERN.matcher(sql.toLowerCase());

        while (SQL_Index_MATCHER.find()){
            sql = SQL_Index_MATCHER.replaceAll("index_");
        }

        return sql;
    }

    /**
     * 合并两个文件
     * @param
     * @param
     */
    public void mergeParsedSQLFile(String readFilePath,String writeFilePath) throws IOException {
        BufferedReader br = new BufferedReader (new InputStreamReader(new FileInputStream(readFilePath),"UTF-8"));
        BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(writeFilePath,true),"UTF-8"));

        String str =null;
        while ((str = br.readLine()) != null){
            bw.write(str+"\n");
        }

        bw.flush();
        bw.close();
        br.close();
    }
}
