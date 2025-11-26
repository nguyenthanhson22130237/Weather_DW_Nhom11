package vn.edu.hcmuaf.fit.web.dao;

import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

// Tạo một class đơn giản để chứa dữ liệu hiển thị
public class DashboardDAO {
    private final ConfigManager config;

    public DashboardDAO(ConfigManager config) {
        this.config = config;
    }

    // Class nội bộ để map dữ liệu query
    public static class WeatherData {
        public String cityName;
        public Date date;
        public double avgTemp;
        public double humidity;
        public String condition;

        public WeatherData(String cityName, Date date, double avgTemp, double humidity, String condition) {
            this.cityName = cityName;
            this.date = date;
            this.avgTemp = avgTemp;
            this.humidity = humidity;
            this.condition = condition;
        }
    }

    // Lấy danh sách thời tiết để hiển thị bảng
    public List<WeatherData> getRecentWeather() {
        List<WeatherData> list = new ArrayList<>();
        String sql = "SELECT c.city_name, d.full_date, f.avg_temp, f.avg_humidity, f.condition_text " +
                "FROM fact_weather f " +
                "JOIN city_dim c ON f.city_id = c.id " +
                "JOIN date_dim d ON f.date_id = d.id " +
                "ORDER BY d.full_date DESC LIMIT 20";

        try (Connection conn = DriverManager.getConnection(config.getWarehouseUrl(), config.getDbUserCommon(), config.getDbPasswordCommon());
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                list.add(new WeatherData(
                        rs.getString("city_name"),
                        rs.getDate("full_date"),
                        rs.getDouble("avg_temp"),
                        rs.getDouble("avg_humidity"),
                        rs.getString("condition_text")
                ));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}