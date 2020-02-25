package com.wrp.jdbc;

import java.sql.*;

/**
 * @ClassName HiveJDBCClient
 * @Author LYleonard
 * @Date 2020/2/25 18:35
 * @Description TODO
 * Version 1.0
 **/
public class HiveJDBCClient {

    private static String url="jdbc:hive2://192.168.29.102:10000/hivedb";

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        // 获取数据库连接
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, "hadoop", "");
            // 定义查询的sql语句
            String sql = "select * from stu";

            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()){
                // 获取id字段
                int id = resultSet.getInt(1);

                // name字段
                String name = resultSet.getString(2);
                System.out.println(id + "\t" + name);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
