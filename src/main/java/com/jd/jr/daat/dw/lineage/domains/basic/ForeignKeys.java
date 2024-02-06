package com.jd.jr.daat.dw.lineage.domains.basic;

import lombok.AllArgsConstructor;
import lombok.Data;

import com.jd.jr.daat.dw.lineage.domains.lineage.table.TableColumnLineage;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ForeignKeys {
    private TableColumnLineage tableColumnLineage;
    private ArrayList<String[]> arrayList;
}



