package com.jd.jr.daat.dw.lineage.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class SchemaExtractorTest {
    private SchemaExtractor schemaExtractor;
    private String resourcePath;
    private String schemaResourcePath;
    private String schemaExtractColunmnResourcePath;
    private String createSchemTablePath;
    @BeforeEach
    void init(){
        schemaExtractor = new SchemaExtractor();
        resourcePath = getClass().getClassLoader().getResource("").getPath();
        schemaResourcePath = resourcePath + "/nebulae_column.txt";
        //schemaExtractColunmnResourcePath = resourcePath +"/nebulae_extract_column.txt";
        createSchemTablePath = resourcePath +"tables/schemaTable";
    }


    @Test
    void testcreateTableSchema() throws IOException {
        schemaExtractor.createTableSchema(schemaResourcePath,createSchemTablePath);
    }
}