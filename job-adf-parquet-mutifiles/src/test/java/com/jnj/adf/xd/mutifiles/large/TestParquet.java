package com.jnj.adf.xd.mutifiles.large;

import org.apache.parquet.hadoop.ParquetUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TestParquet {

    public static String columnNames = "recordSource,orderCompany,orderType,orderNo,orderLineNo,catalogCode,recommendedShipDateJulian,scheduleNumber";
    public static Map<String, String> columnsMap = new HashMap<>();
    public static String parquetPath = "C:\\tmp\\parquet\\aera\\aera-poc_test_118673_0.parquet";
    public static String outPath = "C:\\tmp\\parquet\\aera\\aera-poc_test_118673_0_1.parquet";
    static {
        columnsMap.put("orderLineNo", "order_line_no");
    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, IOException {
        Types.MessageTypeBuilder typeBuilder = Types.buildMessage();
        String[] columns = columnNames.split(",");
        for (String col : columns) {
            String columnName = getRealColumnName(col);
            Type t0 = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, columnName);
            typeBuilder.addField(t0);
        }
        MessageType schema = typeBuilder.named("test");

        ParquetUtil.convertParquetByColumns(parquetPath, outPath, ParquetUtil.makeFilter("*:*"), schema, "a,b,c",columnsMap);
    }

    public static String getRealColumnName(String oldCol) {
        if (columnsMap.containsKey(oldCol)) {
            return columnsMap.get(oldCol);
        }
        return oldCol;
    }
}
