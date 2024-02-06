package com.jd.jr.daat.dw.lineage.domains.basic;

import com.alibaba.druid.sql.ast.SQLObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MethodInvoke {
    private SQLObject sqlObject;
    private String ColumnNameOrAlias;
}
