package com.atguigu.gmall.realtime.common.function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.Set;

@FunctionHint(output = @DataTypeHint("row<keyword string>"))
public class KwSplit extends TableFunction<Row> {
    public void eval(String kw) {
        Set<String> keywords = IkUtil.split(kw);
        for (String keyword : keywords) {
            collect(Row.of(keyword));
        }
    }
}
