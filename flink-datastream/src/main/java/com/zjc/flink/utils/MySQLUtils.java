package com.zjc.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/17
 * @description :
 */
public class MySQLUtils {
    
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public static void close(Connection connection, PreparedStatement pstmt) {
        if(null != pstmt) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        if(null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) {
        System.out.println(getConnection());
    }
    
}
