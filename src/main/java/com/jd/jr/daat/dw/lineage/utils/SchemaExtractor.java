package com.jd.jr.daat.dw.lineage.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;

public class SchemaExtractor {
    public void createTableSchema(String schemaResourcePath ,String createSchemaTablePath ) throws IOException {
        File schemaColumnFile = new File(schemaResourcePath);
//        File schemaCreateTableFile =new File(createSchemaTablePath);
//        FileOutputStream schemaOutPut = new FileOutputStream(schemaCreateTableFile);
        BufferedWriter schemaWriter = new BufferedWriter (new OutputStreamWriter(new FileOutputStream (createSchemaTablePath,true),"UTF-8"));

//        FileWriter schemaWriter = new FileWriter(createSchemaTablePath);
        LineIterator it = FileUtils.lineIterator(schemaColumnFile, "UTF-8");
        String beforeDatabaseNameTableName ="";
//        ArrayList schemaList = new ArrayList();
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] column = line.split("\t");
                String databaseNameTableName  = column[9]+"."+column[7];
                String columnName = column[1];
                String columnType = column[3];

                if (columnType==null){
                    columnType = "STRING";
                }


                String columnItem = createColumnItem(columnName,columnType);

                //如果该表名等于上一个表，说明该表还有字段
                if (databaseNameTableName.equals(beforeDatabaseNameTableName)){
//                    FileUtils.write(schemaCreateTableFile,columnItem ,"UTF-8",true);
                    schemaWriter.write(columnItem);
                } else {
                    //如果该表名不等于上一个表，说明该表没有字段了或者该表只含有一个字段,结束上个表创建并创建一个新表。
                    String tableInfoItem = createTableInfo(databaseNameTableName) ;
                    String endSymbol = createEndSymbol();
                    String schema = endSymbol + tableInfoItem + columnItem;
                    schemaWriter.write(schema);
//                    FileUtils.write(schemaCreateTableFile, endSymbol , "UTF-8", true);
//                    FileUtils.write(schemaCreateTableFile, tableInfoItem , "UTF-8", true);
//                    FileUtils.write(schemaCreateTableFile, columnItem , "UTF-8", true);
                }

                beforeDatabaseNameTableName = databaseNameTableName ;
            }

            //结束最后一个表的创建。
            String endSymbol = createEndSymbol();
//            FileUtils.write(schemaCreateTableFile, endSymbol, "UTF-8", true);
            schemaWriter.write(endSymbol);
            schemaWriter.flush();
            schemaWriter.close();
        } finally {
            LineIterator.closeQuietly(it);
        }
    }

    public String createColumnItem(String columnName,String columnType){
        return  columnName +" " + columnType + "," + "\n";
    }

    public String createTableInfo(String databaseNameTableName){
        return "CREATE TABLE IF NOT EXISTS  " + databaseNameTableName + "\n" + "("+ "\n";
    }

    public String createEndSymbol(){
        return ");"+ "\n";
    }
}