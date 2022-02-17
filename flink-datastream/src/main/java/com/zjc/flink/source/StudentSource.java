package com.zjc.flink.source;

import com.zjc.flink.transformation.Student;
import com.zjc.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/17
 * @description :
 */
public class StudentSource extends RichSourceFunction<Student> {
    
    Connection connection;
    PreparedStatement psmt;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        psmt = connection.prepareStatement("select * from student");
    }
    
    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, psmt);
    }
    
    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet rs = psmt.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            int age = rs.getInt("age");
            sourceContext.collect(new Student(id, name, age));
        }
    }
    
    @Override
    public void cancel() {
    
    }
}
