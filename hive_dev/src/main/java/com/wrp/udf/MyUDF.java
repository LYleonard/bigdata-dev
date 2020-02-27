package com.wrp.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @ClassName MyUDF
 * @Author LYleonard
 * @Date 2020/2/27 22:06
 * @Description 自定义函数开发
 * Version 1.0
 **/
public class MyUDF extends UDF {
    public Text evaluate(final Text s){
        if (null == s){
            return null;
        }
        // 返回大写字母
        return new Text(s.toString().toUpperCase());
    }
}
