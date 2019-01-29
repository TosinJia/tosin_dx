package utils;

import java.sql.*;

public class JDBCUtils {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://bd-101:3306/db_telecom...";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSSWORD = "TosinJia_1";

    public static Connection getConnection(){
        try {
            Class.forName(MYSQL_DRIVER_CLASS);
            return DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 释放资源
     * @param connection
     * @param statement
     * @param resultSet
     */
    public static void close(Connection connection, Statement statement, ResultSet resultSet){
        try {
            if(resultSet != null && !resultSet.isClosed()){
                resultSet.close();
            }
            if(statement != null && !statement.isClosed()){
                statement.close();
            }
            if(connection != null && !connection.isClosed()){
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
