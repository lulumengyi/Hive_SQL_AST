package com.jd.jr.daat.dw.lineage.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SQLExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLExtractor.class.getName());

    // 日志前缀默认分隔符模式字符串
    static final String LOG_PREFIX_DELIMITER_PATTERN_STR = "\\|{3}";

    // 日志前缀默认元素模式字符串
    static final String LOG_PREFIX_ELEMENT_PATTERN_STR = "[A-Za-z0-9_]{1,100}";

    // 日志前缀默认日期模式字符串
    static final String LOG_PREFIX_DATE_PATTERN_STR = "\\d{8}";

    // 日志前缀默认时间模式字符串
    static final String LOG_PREFIX_TIME_PATTERN_STR = "\\(\\d{2}:\\d{2}:\\d{5}\\)";

    // 日志前缀默认 SESSION ID 模式字符串
    static final String LOG_PREFIX_SESSION_ID_PATTERN_STR = "\\d{1,10}";

    // 日志前缀默认 SEQUENCE NO. 模式字符串
    static final String LOG_PREFIX_SEQ_NO_PATTERN_STR = "\\d{1,10}";

    // 日志前缀模式字符串
    static final String LOG_PREFIX_PATTERN_STR = String.format(
            "(?<=^%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s\\s{1}).+$",
            LOG_PREFIX_ELEMENT_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_ELEMENT_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_ELEMENT_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_ELEMENT_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_DATE_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_SESSION_ID_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_SEQ_NO_PATTERN_STR, LOG_PREFIX_DELIMITER_PATTERN_STR,
            LOG_PREFIX_TIME_PATTERN_STR);

    // 日志前缀模式
    static final Pattern LOG_PREFIX_PATTERN = Pattern.compile(LOG_PREFIX_PATTERN_STR);

    // 默认日志换行符
    static final String LOG_DEFAULT_EOL = "\n";

    // 日志内容 SQL 头部关键词
    static final String LOG_CONTENT_SQL_HEAD_KEYWORDS = "Beging Execute Hive Sql".toUpperCase();

    // 日志内容 SQL 尾部关键词
    static final String LOG_CONTENT_SQL_TAIL_KEYWORDS = "End Execute Hive Sql".toUpperCase();

    List<List<String>> sqls = Lists.newArrayList();
    String writeSqlFileName;
    String resourcesPath;

    /**
     * Description:
     *   从单行日志中抽取日志内容
     *
     * @param singleLineLog 单行日志
     * @return 日志内容
     */
    public String extractSingleLineLogContent(String singleLineLog) {
        Matcher logPrefixMatcher = LOG_PREFIX_PATTERN.matcher(singleLineLog);

        String logContent = "";

        if (logPrefixMatcher.find()) {
            logContent = logPrefixMatcher.group();
        }

        return logContent;
    }

    /**
     * Description:
     *   从多行日志中抽取日志内容
     *
     * @param multiLinesLog 多行日志
     * @return 日志内容列表
     */
    public List<String> extractMultiLinesLogContent(String multiLinesLog) {
        String[] logs = multiLinesLog.split(LOG_DEFAULT_EOL);

        return Arrays.stream(logs)
                .map(this::extractSingleLineLogContent)
                .collect(Collectors.toList());
    }

    /**
     * Description:
     *   判断日志内容是否为 SQL 头部
     *
     * @param logContent 日志内容
     * @return 是否为 SQL 头部
     */
    public boolean isSQLHead(String logContent) {
        return logContent.toUpperCase().contains(LOG_CONTENT_SQL_HEAD_KEYWORDS);
    }

    /**
     * Description:
     *   判断日志内容是否为 SQL 尾部
     *
     * @param logContent 日志内容
     * @return 是否为 SQL 尾部
     */
    public boolean isSQLTail(String logContent) {
        return logContent.toUpperCase().contains(LOG_CONTENT_SQL_TAIL_KEYWORDS);
    }

    /**
     * Description:
     *   从日志内容集合中抽取 SQLs
     *
     * @param logContents 日志内容列表
     * @return SQLs
     */
    public List<String> extractLogContentsSQLs(List<String> logContents) {
        List<String> sqls = Lists.newArrayList();

        boolean reachSQLHead = false;
        boolean reachSQLTail = false;
        StringBuffer sqlBuffer = new StringBuffer();

        for (String logContent : logContents) {
            reachSQLTail = isSQLTail(logContent) || reachSQLTail;

            if (reachSQLHead && reachSQLTail) {
                sqls.add(sqlBuffer.toString());
                sqlBuffer = new StringBuffer();

                reachSQLHead = false;
                reachSQLTail = false;
            }

            if (reachSQLHead && !reachSQLTail) {
                sqlBuffer.append(logContent);
                sqlBuffer.append("\n");
            }

            reachSQLHead = isSQLHead(logContent) || reachSQLHead;
        }

        return sqls.stream()
                .map(sql -> sql.split(";"))
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
    }

    /**
     * Description:
     *   从多行日志中抽取 SQLs
     *
     * @param multiLinesLog 多行日志
     * @return SQLs
     */
    public List<String> extractLogSQLs(String multiLinesLog) {
        List<String> logContents = extractMultiLinesLogContent(multiLinesLog);
        return extractLogContentsSQLs(logContents);
    }


    /**
     * Description:
     *     从该路径下的多个文件中抽取SQLs
     * @param path 文件路径
     *
     */
    public void extractPathLogSQLs(String path ,BufferedWriter sqlWriter) throws IOException {
        //获取其file对象
        File file = new File(path);
        isFilePath(file,sqlWriter);
        sqlWriter.flush();
        sqlWriter.close();
    }

    public void isFilePath(File file,BufferedWriter sqlWriter) throws IOException {
        File[] fs = file.listFiles();

        for(File f:fs){
            if(f.isDirectory()){
                //若是目录，则递归遍历该目录下的文件
                isFilePath(f,sqlWriter);
            }

            if(f.isFile()){
                //若是文件，获取单个文件的sql
                extractSingleFilesLogSQLs(f.toString(),sqlWriter);
                System.out.println(String.format("extracting sql from log file [%s] ...", f.toString()));
            }

        }


    }

    /**
     * Decription:
     *  获取单个文件的SQL
     * @param filename 文件名
     * @return sql
     * @throws IOException 抛异常
     */
    public List<String> extractSingleFilesLogSQLs(String filename,BufferedWriter sqlWriter) throws IOException {
        //List<String> logsEAPFLines = FileUtils.readLines(new File(filename), "UTF-8");
        List<String> logsEAPFLines = Lists.newArrayList();
        BufferedReader br = new BufferedReader (new InputStreamReader(new FileInputStream (filename),"UTF-8"));
        String str = null;

        while ((str = br.readLine()) != null){
            logsEAPFLines.add(str+"");
        }

        List<String> sql = extractLogSQLs(Joiner.on("\n").join(logsEAPFLines));
        sql.stream().forEach(s -> {
            try {
                sqlWriter.write(s+";");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return sql;

    }
}
