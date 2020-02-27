package com.wrp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @ClassName MyJsonParser
 * @Author LYleonard
 * @Date 2020/2/27 22:36
 * @Description TODO
 * Version 1.0
 **/
public class MyJsonParser extends UDF {
    public Text evaluate(final Text s){
        if (null == s){
            return null;
        }
        String line = s.toString().replace("{", "").replace("}", "");
        String[] items = line.split(",");
        String result = "";
        for (String item : items){
            if (result.equals("")){
                result = item;
            }else {
                result += ("\t" +item.split(":")[1]);
            }
        }
        return new Text(result);
    }
}
