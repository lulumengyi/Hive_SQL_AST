package com.jd.jr.daat.dw.lineage.utils;

import com.alibaba.druid.sql.repository.SchemaRepository;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SchemaLoaderTest {
    private SchemaLoader schemaLoader;
    private SQLExtractor sqlExtractor;
    private String logsFilePath;

    @BeforeEach
    void init() throws Exception {
        schemaLoader = new SchemaLoader();
        sqlExtractor = new SQLExtractor();

        String resourcesPath = getClass().getClassLoader().getResource("").getPath();
        logsFilePath = resourcesPath + "/logs";

    }

    @Test
    void testLoad() throws IOException {


        //List<List<String>> sqls = sqlExtractor.extractPathLogSQLs(logsFilePath);
        //SchemaRepository schemaRepository = schemaLoader.load(sqls, JdbcConstants.HIVE);
//
//        assertEquals(4, schemaRepository.getSchemas().size());
//        assertEquals(1, schemaRepository.findSchema("dmr").getTableCount());
//        assertEquals(10, schemaRepository.findSchema("tmp").getTableCount());
    }
}
