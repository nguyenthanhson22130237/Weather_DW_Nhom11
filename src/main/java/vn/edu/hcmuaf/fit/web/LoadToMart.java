package vn.edu.hcmuaf.fit.web;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoadToMart {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/";
    private static final String DB_USER = "etl_user";
    private static final String DB_PASS = "123456";

    public LoadToMart() {
    }

    public static void main(String[] args) {
        Connection conn = null;
        int logId = -1;

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/?allowPublicKeyRetrieval=true&useSSL=false", "etl_user", "123456");
            System.out.println("Connected to Database.");
            logId = insertLog(conn, "LoadToMart", "RUNNING");
            loadDataToMart(conn);
            updateLog(conn, logId, "SUCCESS");
            System.out.println("Data Mart Loaded Successfully!");
        } catch (Exception e) {
            e.printStackTrace();
            if (conn != null && logId != -1) {
                updateLog(conn, logId, "FAILURE");
            }
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

    }

    private static void loadDataToMart(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("TRUNCATE TABLE weather_datamart.monthly_report");
        String sql = "INSERT INTO weather_datamart.monthly_report (city_name, country, month, year, avg_temp_monthly, max_temp_monthly, min_temp_monthly, record_count) SELECT    c.city_name,    c.country,    d.month,    d.year,    AVG(f.avg_temp),    MAX(f.max_temp),    MIN(f.min_temp),    COUNT(f.id) FROM weather_wh.fact_weather f JOIN weather_wh.date_dim d ON f.date_id = d.id JOIN weather_wh.city_dim c ON f.city_id = c.id GROUP BY c.city_name, c.country, d.month, d.year;";
        int rows = stmt.executeUpdate(sql);
        System.out.println("Inserted " + rows + " rows into Data Mart.");
        stmt.close();
    }

    private static int insertLog(Connection conn, String step, String status) {
        int id = -1;

        try {
            String sql = "INSERT INTO log.etl_log (step_name, start_time, status, log_date) VALUES (?, NOW(), ?, CURDATE())";
            PreparedStatement pstmt = conn.prepareStatement(sql, 1);
            pstmt.setString(1, step);
            pstmt.setString(2, status);
            pstmt.executeUpdate();
            ResultSet rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
                id = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return id;
    }

    private static void updateLog(Connection conn, int id, String status) {
        try {
            String sql = "UPDATE log.etl_log SET end_time = NOW(), status = ? WHERE id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, status);
            pstmt.setInt(2, id);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
