package com.wrp.phoenix;

import java.sql.*;

/**
 * @ClassName PhoenixJDBCSearch
 * @Author LYleonard
 * @Date 2020/3/22 16:33
 * @Description Phoenix JDBC测试
 * Version 1.0
 **/
public class PhoenixJDBCSearch {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:phoenix:master:2181";
        Connection connection = DriverManager.getConnection(url);
        Statement statement = connection.createStatement();

        String sql = "select * from US_POPULATION";
        //执行sql
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()){
            System.out.println("state: " + resultSet.getString("state"));
            System.out.println("city: " + resultSet.getString("city"));
            System.out.println("population: " + resultSet.getInt("population"));
            System.out.println("---------------------------------------");
        }
        connection.close();
    }
}
