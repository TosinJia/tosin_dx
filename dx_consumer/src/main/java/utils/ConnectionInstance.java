package utils;

import java.sql.Connection;

public class ConnectionInstance {
    private static Connection connection;
    public static synchronized Connection getConnection(Configuration configuration){
        if(connection  == null || connection.isClosed()){
            connection =
        }
        return connection;
    }
}
