package com.jd.jr.daat.dw.lineage.utils;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ProcessUnparsedSQLTest {
    private ProcessUnparsedSQL SQLProcess;
    String resourcesPath = getClass().getClassLoader().getResource("").getPath();
    private List<String> sqlLineageDemo01;

    String parsedSplitSQLPath = resourcesPath +"process_unparsed/SubParsed.sql";
    String unParsedSQLPath = resourcesPath +"process_unparsed/SubUnParsed.sql";

    /**
     * 从文件中得到未解析的sql
     * @param
     */
    @Test
    void getParsedSQL() throws IOException {
        BufferedWriter parsedSplitSQLWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(parsedSplitSQLPath,true),"UTF-8"));
        BufferedWriter unParsedWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(unParsedSQLPath,true),"UTF-8"));

        sqlLineageDemo01 = Lists.newArrayList(FileUtils.readFileToString(new File(resourcesPath +
                "process_unparsed/GetUnParsed.sql"), "UTF-8").split(";"));
        //过滤掉不能解析的sql语句
        for(String sql:sqlLineageDemo01){
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
            if(!sql.equals("")){
                if(beforeSQL.equals(sql)){
                    unParsedWriter.write(sql+";"+"\n");
                }else{
                    parsedSplitSQLWriter.write(sql+";"+"\n");
                }
            }

        }
        unParsedWriter.flush();
        unParsedWriter.close();
        parsedSplitSQLWriter.flush();
        parsedSplitSQLWriter.close();

    }

    /**
     * 处理split函数，把split函数去掉,留下中间的字段
     *
     */

    public  String processUnparsedSplitSQL(String sql ) throws IOException {
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

    public String  processUnparsedSortSQL(String sql){
        //String sql ="row_Number() over(distribute by item_first_cate_id,item_second_cate_id,item_third_cate_id sort by ratio desc,good_comment_ratio desc,price) rank_sku\n";
        final String SQL_SORT_REGEX_STR ="(row_number)\\s*\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(rank)\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(count)\\(1\\)\\s*over\\s*\\(.*\\)\\s*(as)*|(sum\\(\\w+\\))\\(\\)\\s*over\\s*\\(.*\\)\\s*(as)*";

        String repalce_SQL="";
        Pattern SQL_SORT_PATTERN=Pattern.compile(SQL_SORT_REGEX_STR);
        Matcher SQL_SORT_MATCHER = SQL_SORT_PATTERN.matcher(sql.toLowerCase());

        while(SQL_SORT_MATCHER.find()){
            repalce_SQL = (SQL_SORT_MATCHER.replaceAll(""));
        }
        if(!repalce_SQL.equals("")){
            sql=repalce_SQL;
        }
        return sql;

    }

    /**
     *
     * @param sql
     * @return
     */
    public String  processUnparsedBracketSQL(String sql){
        //String sql ="row_Number() over(distribute by item_first_cate_id,item_second_cate_id,item_third_cate_id sort by ratio desc,good_comment_ratio desc,price) rank_sku\n";
        final String SQL_SORT_REGEX_STR ="\\[\\d+\\]";

        String repalce_SQL="";
        Pattern SQL_SORT_PATTERN=Pattern.compile(SQL_SORT_REGEX_STR);
        Matcher SQL_SORT_MATCHER = SQL_SORT_PATTERN.matcher(sql.toLowerCase());

        while(SQL_SORT_MATCHER.find()){
            repalce_SQL = (SQL_SORT_MATCHER.replaceAll(""));
        }
        if(!repalce_SQL.equals("")){
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

        while(SQL_OVER_DISTRIBUTE_BY_MATCHER.find()){
//            System.out.println("ok");
//            System.out.println(SQL_DISTRIBUTE_BY_MATCHER.group());
            sql = SQL_OVER_DISTRIBUTE_BY_MATCHER.replaceAll("");
        }
        while(SQL_DISTRIBUTE_BY_MATCHER.find()){
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


        if(SQL_CREATE_MATCHER.find()&&!SQL_SELECT_MATCHER.find()){
            System.out.println("ok");
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

        while(SQL_GROUP_MATCHER.find()){

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

        while(SQL_OVER_MATCHER.find()){
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

        while(SQL_OVER_MATCHER.find()){
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

        while(SQL_Integer_MATCHER.find()){
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

        while(SQL_Index_MATCHER.find()){
            sql = SQL_Index_MATCHER.replaceAll("index_");
        }
        return sql;
    }


    @Test
    public void processUnparsedCreateSQL1(){
        String sql = "CREATE TABLE IF NOT EXISTS DWD.DWD_O_MY1_MGR_USER_I_I_D(\n" +
                "     etl_dt    string     comment 'ETL日期'\n" +
                "    ,jrjt_del_dt    string     comment '删除日期'\n" +
                "    ,id    bigint     comment '自增主键'\n" +
                "    ,select_pwd    string     comment '数据源用户密码查询秘钥'\n" +
                "    ,erp    string     comment '用户erp'\n" +
                "    ,user_name    string     comment '用户姓名(中文)'\n" +
                "    ,email    string     comment '用户邮箱'\n" +
                "    ,tel    string     comment '联系电话'\n" +
                "    ,designated_person    string     comment '任务指派人'\n" +
                "    ,department_id    string     comment '关联部门id'\n" +
                "    ,create_time    string     comment '用户创建时间')COMMENT '201801081433022 测试任务'\n" +
                "PARTITIONED BY (dt string COMMENT '日期分区')\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n" +
                "STORED AS ORC\n" +
                ";";
        final String SQL_CREATE_REGEX_STR ="create table if not exists";
        final String SQL_REGEX_STR = "select";
        String repalce_SQL="";
        Pattern SQL_CREATE_PATTERN=Pattern.compile(SQL_CREATE_REGEX_STR);
        Matcher SQL_CREATE_MATCHER = SQL_CREATE_PATTERN.matcher(sql.toLowerCase());
        Pattern SQL_PATTERN=Pattern.compile(SQL_REGEX_STR);
        Matcher SQL_MATCHER = SQL_PATTERN.matcher(sql.toLowerCase());

//        if(SQL_CREATE_MATCHER.find()){
//            System.out.println(SQL_CREATE_MATCHER.group());
//            System.out.println("ok");
//            sql = repalce_SQL;
//        }
        if(SQL_CREATE_MATCHER.find()&&!SQL_MATCHER.find()){
            System.out.println(SQL_CREATE_MATCHER.group());
            System.out.println("ok");
            sql = repalce_SQL;
        }
        System.out.println(sql.toLowerCase());
    }
    @Test
    public void processUnparsedSortSQL1(){
        String sql =" count(1) over(distribute by new_type,sdtperiod sort by score_all desc) rsc,";
        final String SQL_SORT_REGEX_STR ="(row_number)\\(\\)\\sover\\(.*\\)\\s(as)*|(rank)\\(\\)\\sover\\(.*\\)\\s(as)*|(count)\\(1\\)\\sover\\(.*\\)\\s(as)*";
        final String SQL_RANK_REGEX_STR ="(rank\\(\\)\\sover\\().*?\\)\\s(as)*";
        String repalce_SQL="";
        Pattern SQL_SORT_PATTERN=Pattern.compile(SQL_SORT_REGEX_STR);
        Matcher SQL_SORT_MATCHER = SQL_SORT_PATTERN.matcher(sql.toLowerCase());

        Pattern SQL_RANK_PATTERN=Pattern.compile(SQL_RANK_REGEX_STR);
        Matcher SQL_RANK_MATCHER = SQL_RANK_PATTERN.matcher(sql.toLowerCase());


        if(SQL_SORT_MATCHER.find()){
            System.out.println(SQL_SORT_MATCHER.group());

            sql=SQL_SORT_MATCHER.replaceAll("");
        }
//        if(SQL_RANK_MATCHER.find()){
//            System.out.println(SQL_RANK_MATCHER.group());
//
//            sql=SQL_RANK_MATCHER.replaceAll("");
//        }
        System.out.println(sql);

    }

    /**
     * 处理含有grouping sets语句，找到该语句用空格代替
     * @param
     * @return
     */
    @Test
    public void processUnparsedGroupingSets1(){
        String sql ="grouping sets ((dim_day),(dim_day,investor))";
        String SQL_GROUP_REGEX_STR ="grouping sets\\s*\\n*\\([^\\(\\)]*(\\([^\\(\\)]*\\)[^\\(\\)]*)*\\)";

        Pattern SQL_GROUP_PATTERN=Pattern.compile(SQL_GROUP_REGEX_STR);
        Matcher SQL_GROUP_MATCHER = SQL_GROUP_PATTERN.matcher(sql);

        String SQL_GROUP_REGEX_STR_1 = "grouping sets\\s*\\n*\\([^\\(\\)]*(\\([^\\(\\)]*(\\([^\\(\\)]*\\)[^\\(\\)]*)*\\)[^\\(\\)]*)*\\)";

        Pattern SQL_GROUP_PATTERN_1=Pattern.compile(SQL_GROUP_REGEX_STR_1);
        Matcher SQL_GROUP_MATCHER_1 = SQL_GROUP_PATTERN_1.matcher(sql);


        while(SQL_GROUP_MATCHER.find()){
            System.out.println("ok");
            System.out.println(SQL_GROUP_MATCHER.group());
            sql = SQL_GROUP_MATCHER.replaceAll("");
        }
        while(SQL_GROUP_MATCHER_1.find()){
            System.out.println("ok");
            System.out.println(SQL_GROUP_MATCHER_1.group());
            sql = SQL_GROUP_MATCHER_1.replaceAll("");
        }
        System.out.println(sql);


    }
    //测试
    @Test
    void processUnparsedSQL() throws IOException {
        //String sql = "split(regexp_replace(requesturldecode,'^http[s]?://m.jr.jd.com/mjractivity/',''),'-')[0] id";
        String sql ="split(csl_col_parser(value,'jcd'), \"\\\\\\\\||\\\\%7C\")[0] in ('',' ','null','NULL') then null else split(csl_col_parser(value,'jcd'), \"\\\\\\\\||\\\\%7C\")[0] end as deviceid_base";
        final String SQL_SPLIT_PREFIX_REGEX_STR ="split\\s*\\(";
        final String SQL_SPLIT_SUFFFIX_REGEX_STR ="\\,\\s*\'*[a-zA-Z\\d\\_\\-\"|%#&.,:*?{}()=\\[\\]\\\\]{1,30}\\s*\'*\\s*\\)\\s*\\[\\d\\]";

        Pattern SQL_SPLIT_PREFIX_PATTERN=Pattern.compile(SQL_SPLIT_PREFIX_REGEX_STR);
        Matcher SQL_SPLIT_PREFIX_MATCHER = SQL_SPLIT_PREFIX_PATTERN.matcher(sql);

        Pattern SQL_SPLIT_SUFFIX_PATTERN=Pattern.compile(SQL_SPLIT_SUFFFIX_REGEX_STR);
        String s="";
        String finalSQL="";
        while(SQL_SPLIT_PREFIX_MATCHER.find()){
            System.out.println(SQL_SPLIT_PREFIX_MATCHER.group(0));
            s =SQL_SPLIT_PREFIX_MATCHER.replaceAll("");
        }

        Matcher SQL_SPLIT_SUFFIX_MATCHER = SQL_SPLIT_SUFFIX_PATTERN.matcher(s);
        while(SQL_SPLIT_SUFFIX_MATCHER.find()){
            finalSQL = SQL_SPLIT_SUFFIX_MATCHER.replaceAll("");
            System.out.println(finalSQL);
        }


    }

    @Test
    public void processUnparsedDistributeBy1(){
        String sql="distribute by dt";

        String SQL_DISTRIBUTE_BY_REGEX_STR = "distribute by\\s\\w+";
        Pattern SQL_DISTRIBUTE_BY_PATTERN= Pattern.compile(SQL_DISTRIBUTE_BY_REGEX_STR);
        Matcher SQL_DISTRIBUTE_BY_MATCHER = SQL_DISTRIBUTE_BY_PATTERN.matcher(sql);

        while(SQL_DISTRIBUTE_BY_MATCHER.find()){
            System.out.println("ok");
            System.out.println(SQL_DISTRIBUTE_BY_MATCHER.group());
            sql = SQL_DISTRIBUTE_BY_MATCHER.replaceAll("");
        }
        System.out.println(sql);

    }

    /**
     *
     */
    public void processUnparsedIntegerSQL(){
        String sql = "";

        String SQL_DISTRIBUTE_BY_REGEX_STR = "select";
        Pattern SQL_DISTRIBUTE_BY_PATTERN= Pattern.compile(SQL_DISTRIBUTE_BY_REGEX_STR);
        Matcher SQL_DISTRIBUTE_BY_MATCHER = SQL_DISTRIBUTE_BY_PATTERN.matcher(sql);

        while(SQL_DISTRIBUTE_BY_MATCHER.find()){
            System.out.println("ok");
            System.out.println(SQL_DISTRIBUTE_BY_MATCHER.group());
            sql = SQL_DISTRIBUTE_BY_MATCHER.replaceAll("");
        }
        System.out.println(sql);

    }


}