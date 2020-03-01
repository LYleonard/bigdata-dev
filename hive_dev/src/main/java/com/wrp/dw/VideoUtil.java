package com.wrp.dw;

/**
 * @ClassName VideoUtil
 * @Author LYleonard
 * @Date 2020/3/1 14:04
 * @Description 初步清洗数据
 *      *   数据切割，如果长度小于9 直接丢掉
 *      *   视频类别中间空格 去掉
 *      *   关联视频，使用 &  进行分割
 * Version 1.0
 **/
public class VideoUtil {
    public static String washDatas(String line){
        if (null == line || "".equals(line)){
            return null;
        }

        // 判断数据的长度，如果小于9，删除
        String[] fields = line.split("\t");
        int fieldsLength = fields.length;
        if (fieldsLength < 9){
            return null;
        }

        fields[3] = fields[3].replace(" ", "");
        StringBuilder builder = new StringBuilder();
        for (int i=0; i< fieldsLength; i++){
            if (i < 9){
                builder.append(fields[i]).append("\t");
            } else if (i >= 9 && i < fieldsLength-1){
                builder.append(fields[i]).append("&");
            } else if (i == fieldsLength){
                builder.append(fields[i]);
            }
        }
        return builder.toString();
    }
}
