package com.jd.jr.daat.dw.lineage.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.jd.jr.daat.dw.lineage.analysis.ColumnLineageAnalyzer;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class GetUnparsedSQLTest {
    private SchemaLoader schemaLoader;
    private SQLExtractor sqlExtractor;
    private SchemaRepository schemaRepository;
    private ColumnLineageAnalyzer analyzer;

    private String logsFilePath;
    private List<List<String>> sqlsEAPF;
    private List<String> sqlLineageDemo01;
    private List<String> sqlsLineageDemo01Tables;
    private String FileTableSchemaPath;
    private String unparsedSqlPath;
    private String parsedSqlPath;
    private String writeFilePath;
    private String readFilePath;

    private List<List<String>> Allpath = new ArrayList<List<String>>();

    private List<String> results;
    private int count;
    private int normalCount;


    @BeforeEach
    void init() throws IOException {
        schemaLoader = new SchemaLoader();
        sqlExtractor = new SQLExtractor();

        // 获取当前编译好类的路径
        String resourcesPath = getClass().getClassLoader().getResource("").getPath();

        logsFilePath = resourcesPath + "/logs";

        //sqlsEAPF = sqlExtractor.extractPathLogSQLs(logsFilePath);

        FileTableSchemaPath = resourcesPath + "schemaTable.sql";

        unparsedSqlPath = resourcesPath + "process_unparsed/GetSubUnParsed.sql";
        parsedSqlPath = resourcesPath + "process_unparsed/GetSubParsed.sql";

        writeFilePath = resourcesPath + "process_unparsed/GetSubParsed.sql";
        readFilePath = resourcesPath + "process_unparsed/GetParsed.sql";

        sqlLineageDemo01 = Lists.newArrayList(FileUtils.readFileToString(new File(resourcesPath +
                "process_unparsed/SubParsed.sql"), "UTF-8").split(";"));
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
    public void testGetUnparsedSQL() throws IOException {
        schemaRepository = schemaLoader.load(sqlsLineageDemo01Tables, JdbcConstants.HIVE);

        analyzer = new ColumnLineageAnalyzer(schemaRepository);

        BufferedWriter unparsedSqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(unparsedSqlPath,true),"UTF-8"));
        BufferedWriter parsedSqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(parsedSqlPath,true),"UTF-8"));

        // newStruct是一个存储结构，存储了血缘分析结果tableColumnLineage（原始sql的血缘分析和用主外键构造出sql的血缘分析结果）和最终链接到物理表的主外键对列表

        for (String sql : sqlLineageDemo01) {
            try {
                List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, schemaRepository.getDbType());

                stmtList.forEach(schemaRepository::resolve);
                 for (SQLStatement stmt : stmtList) {
                     parsedSqlWriter.write(sql+";");
                     continue;
                 }
            } catch (com.alibaba.druid.sql.parser.ParserException e) {
                //System.out.println(sql);
                count++;
                System.out.println(count);
                unparsedSqlWriter.write(sql+";");
            }
        }
        unparsedSqlWriter.flush();
        unparsedSqlWriter.close();
        parsedSqlWriter.flush();
        parsedSqlWriter.close();
    }

    /**
     * 合并两个文件
     * @param
     * @param
     */
    @Test
    public void mergeFile() throws IOException {

        BufferedReader br = new BufferedReader (new InputStreamReader(new FileInputStream(readFilePath),"UTF-8"));
        BufferedWriter bw = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(writeFilePath,true),"UTF-8"));
        String str =null;
        while((str = br.readLine()) != null){
            bw.write(str+"\n");
        }
        bw.flush();
        bw.close();
        br.close();
    }

}
