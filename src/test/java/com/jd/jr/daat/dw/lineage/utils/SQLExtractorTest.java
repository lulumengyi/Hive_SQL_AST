package com.jd.jr.daat.dw.lineage.utils;

import com.google.common.base.Joiner;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;

class SQLExtractorTest {
    private SQLExtractor sqlExtractor;
    private List<String> logsEAPFLines;
    private String filename;
    String writeSqlFileName;
    String resourcesPath;
    String logsPath;

    @BeforeEach
    void init() throws Exception {
        sqlExtractor = new SQLExtractor();

        resourcesPath = getClass().getClassLoader().getResource("").getPath();
        logsPath = resourcesPath +"/logs";
        filename = resourcesPath + "/JDW_DMT_WEIXIN_PAYUSER_SEARCH_ACTIVITY_7DAYS-2018-05-15-73.log";
        writeSqlFileName = resourcesPath +"SQLS/sqls_ns_jrdw_internal_1.sql";
        logsEAPFLines = FileUtils.readLines(new File(filename), "UTF-8");

    }

    @Test
    void testExtractSingleLogContent() {
        String logContent_ = "job exe Command...perl /export/jrdw/private/ns_jrdw_internal/" +
                "JDW_DMR_S_EAPF_ENTERPRISE_ADDRESS_RELATION_S_D/JDW_DMR_S_EAPF_ENTERPRISE_ADDRESS_RELATION_S_D.pl" +
                " JDW_JDW_DMR_S_EAPF_ENTERPRISE_ADDRESS_RELATION_S_D_20180515.dir";
        String logContent = sqlExtractor.extractSingleLineLogContent(logsEAPFLines.get(0));
        //assertEquals(logContent_, logContent);
    }

    @Test
    void testExtractMultiLinesLogContent() {
        String multiLinesLog = String.format("%s\n%s", logsEAPFLines.get(3), logsEAPFLines.get(4));
//        List<String> logContents_ = Arrays.asList("Run start...pid:156912", " ");
//        List<String> logContents = sqlExtractor.extractMultiLinesLogContent(multiLinesLog);
//        assertEquals(logContents_, logContents);
    }

    @Test
    void testIsSQLHeadAndTail() {
        List<String> logContents = sqlExtractor.extractMultiLinesLogContent(
                Joiner.on("\n").join(logsEAPFLines));
//        assertEquals(13, logContents.size());
//
//        String logContent0 = logContents.get(0);
//        assertFalse(sqlExtractor.isSQLHead(logContent0));
//        assertFalse(sqlExtractor.isSQLTail(logContent0));
//
//        String logContent1 = logContents.get(1);
//        assertTrue(sqlExtractor.isSQLHead(logContent1));
//        assertFalse(sqlExtractor.isSQLTail(logContent1));

        String logContent11 = logContents.get(11);
//        assertFalse(sqlExtractor.isSQLHead(logContent11));
//        assertTrue(sqlExtractor.isSQLTail(logContent11));
    }

    @Test
    void testExtractLogContentsSQLs() throws IOException {
        List<String> logContents = sqlExtractor.extractMultiLinesLogContent(
                Joiner.on("\n").join(logsEAPFLines));
        List<String> sqls = sqlExtractor.extractLogContentsSQLs(logContents);
        //System.out.println(sqls);
    }

    @Test
    void testExtractLogSQLs() throws IOException {
        List<String> sqls = sqlExtractor.extractLogSQLs(Joiner.on("\n").join(logsEAPFLines));

    }
    @Test
    void testextractPathLogSQLs() throws IOException {
        BufferedWriter sqlWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream(writeSqlFileName,true),"UTF-8"));

        sqlExtractor.extractPathLogSQLs(logsPath,sqlWriter);
//        sqls.stream().forEach(sql -> {
//            sql.stream().forEach(s -> {
//                try {
//                    sqlWriter.write(s+";");
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            });
//        });
//        sqlWriter.flush();
//        sqlWriter.close();


    }

}
