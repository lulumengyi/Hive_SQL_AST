package com.jd.jr.daat.dw.lineage.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.alibaba.druid.sql.repository.SchemaRepository;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SchemaLoader {
    public SchemaRepository load(List<String> sqls, String dbType) {
        SchemaRepository schemaRepository = new SchemaRepository(dbType);

        sqls.forEach(schemaRepository::console);


//        List<SQLStatement> stmtList = sqls.stream()
//                .map(sql -> SQLUtils.parseStatements(sql, dbType))
//                .flatMap(List::stream)
//                .collect(Collectors.toList());
//
//        log.info("Loading table schemas ...");
//
//        long tableCounter = 0;
//        for (SQLStatement stmt : stmtList) {
//            if (stmt instanceof HiveCreateTableStatement) {
//                //schemaRepository.accept(stmt);
//                tableCounter++;
//            }
//        }
//
//        log.info(String.format("%d table schemas loaded.", tableCounter));

        return schemaRepository;
    }
}
