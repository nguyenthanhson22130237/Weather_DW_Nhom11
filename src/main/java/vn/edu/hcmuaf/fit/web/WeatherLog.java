package vn.edu.hcmuaf.fit.wh;

import vn.edu.hcmuaf.fit.wh.ControlManager.ConfigManager;

import java.sql.*;
import java.time.LocalDate;

public class WeatherLog {
    private final ConfigManager config;

    public WeatherLog(ConfigManager config) {
        this.config = config;
    }
    public void run() {
        try {
            //  Đọc file config.xml
            String whDbUrl = config.getWarehouseUrl();
            String dbUser = config.getDbUserCommon();
            String dbPass = config.getDbPasswordCommon();

            // Kết nối DB warehouse
            try (Connection whConn = DriverManager.getConnection(whDbUrl, dbUser, dbPass)) {
                System.out.println("Kết nối warehouse thành công!");

                LocalDate today = LocalDate.now();

                // Lấy toàn bộ bản ghi mới trong fact_weather
                String selectSql = "SELECT id, city_id, date_id, max_temp, min_temp, avg_temp, " +
                                        "avg_humidity, maxwind_kph, uv, rain_chance, condition_text " +
                                "FROM fact_weather";
                try (PreparedStatement selectStmt = whConn.prepareStatement(selectSql);
                     ResultSet rs = selectStmt.executeQuery()) {

                    while (rs.next()) {
                        int factId = rs.getInt("id");
                        int cityId = rs.getInt("city_id");
                        int dateId = rs.getInt("date_id");
                        double maxTemp = rs.getDouble("max_temp");
                        double minTemp = rs.getDouble("min_temp");
                        double avgTemp = rs.getDouble("avg_temp");
                        double avgHumidity = rs.getDouble("avg_humidity");
                        double maxWind = rs.getDouble("maxwind_kph");
                        double uv = rs.getDouble("uv");
                        int rainChance = rs.getInt("rain_chance");
                        String condition = rs.getString("condition_text");

                        // Kiểm tra bản ghi tồn tại
                        String existSql = """
                            SELECT EXISTS (
                                SELECT 1 FROM weather_log 
                                WHERE weather_id = ? 
                                AND city_id = ? 
                                AND date_id = ? 
                                AND max_temp = ? 
                                AND min_temp = ? 
                                AND avg_temp = ? 
                                AND avg_humidity = ? 
                                AND maxwind_kph = ? 
                                AND uv = ? 
                                AND rain_chance = ? 
                                AND condition_text = ?
                                AND date_expired = '9999-12-31'
                            )
                        """;

                        try (PreparedStatement existStmt = whConn.prepareStatement(existSql)) {
                            existStmt.setInt(1, factId);
                            existStmt.setInt(2, cityId);
                            existStmt.setInt(3, dateId);
                            existStmt.setDouble(4, maxTemp);
                            existStmt.setDouble(5, minTemp);
                            existStmt.setDouble(6, avgTemp);
                            existStmt.setDouble(7, avgHumidity);
                            existStmt.setDouble(8, maxWind);
                            existStmt.setDouble(9, uv);
                            existStmt.setInt(10, rainChance);
                            existStmt.setString(11, condition);

                            ResultSet existRS = existStmt.executeQuery();
                            existRS.next();
                            boolean exists = existRS.getBoolean(1);

                            if (exists) {
                                System.out.println("Bỏ qua bản ghi tồn tại cho city_id=" + cityId + ", date_id=" + dateId + " do trùng dữ liệu");
                            } else {
                                // Cập nhật bản cũ (expire)
                                String updateOld = """
                                    UPDATE weather_log
                                    SET date_expired = ?
                                    WHERE city_id = ? AND date_expired = '9999-12-31'
                                """;
                                try (PreparedStatement psUpdate = whConn.prepareStatement(updateOld)) {
                                    psUpdate.setDate(1, Date.valueOf(today));
                                    psUpdate.setInt(2, cityId);
                                    psUpdate.executeUpdate();
                                }
                                // Insert bản mới vào log
                                String insertSql = """
                                    INSERT INTO weather_log 
                                    (weather_id, city_id, date_id, max_temp, min_temp, avg_temp, avg_humidity, maxwind_kph, uv, rain_chance, condition_text, date_created, date_expired)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), '9999-12-31')
                                """;
                                try (PreparedStatement insertStmt = whConn.prepareStatement(insertSql)) {
                                    insertStmt.setInt(1, factId);
                                    insertStmt.setInt(2, cityId);
                                    insertStmt.setInt(3, dateId);
                                    insertStmt.setDouble(4, maxTemp);
                                    insertStmt.setDouble(5, minTemp);
                                    insertStmt.setDouble(6, avgTemp);
                                    insertStmt.setDouble(7, avgHumidity);
                                    insertStmt.setDouble(8, maxWind);
                                    insertStmt.setDouble(9, uv);
                                    insertStmt.setInt(10, rainChance);
                                    insertStmt.setString(11, condition);
                                    insertStmt.executeUpdate();
                                    System.out.println(" Thêm mới log cho city_id=" + cityId + ", date_id=" + dateId);
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        ConfigManager config = new ConfigManager("config.xml");
        WeatherLog log = new WeatherLog(config);
        log.run();
    }
}
