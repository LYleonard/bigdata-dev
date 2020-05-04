package com.wrp;

import java.sql.*;

/**
 * @ClassName ImpalaJdbc
 * @Author LYleonard
 * @Date 2020/5/5 1:26
 * @Description Impala jdbc连接hive
 * Version 1.0
 **/
public class ImpalaJdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //定义连接驱动类
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String driverUrl = "jdbc:hive2://192.168.29.102:21050/impaladb;auth=noSasl";
        String sql = "select * from customer";

        //通过反射加载数据库连接驱动
        Class.forName(driver);
        Connection connection = DriverManager.getConnection(driverUrl);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();

        //通过查询，得到数据一共有多少列
        int colNum = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= colNum; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println("\n");
        }
        preparedStatement.close();
        connection.close();
    }
}
