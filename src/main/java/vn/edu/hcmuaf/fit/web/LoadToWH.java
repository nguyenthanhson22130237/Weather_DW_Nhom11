package vn.edu.hcmuaf.fit.wh;

import vn.edu.hcmuaf.fit.wh.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.wh.ControlManager.ETLRunner;

import java.sql.*;

public class LoadToWH {
    private final ConfigManager config;

    public LoadToWH(ConfigManager config) {
        this.config = config;
    }

    public void run() {
        try {
            // 1. Kết nối tới staging
            Connection stgConn = DriverManager.getConnection(
                    config.getStagingUrl(),
                    config.getDbUserCommon(),
                    config.getDbPasswordCommon()
            );

            // 2. Kết nối tới warehouse
            Connection whConn = DriverManager.getConnection(
                    config.getWarehouseUrl(),
                    config.getDbUserCommon(),
                    config.getDbPasswordCommon()
            );

            // 3. Đọc dữ liệu từ staging
            String sqlSelect = "SELECT city_name, country, timezone, full_date, max_temp, min_temp, avg_temp, avg_humidity, maxwind_kph, uv, rain_chance, condition_text FROM staging_cleaned";
            ResultSet rs = stgConn.createStatement().executeQuery(sqlSelect);

            // 4. Ghi sang warehouse
            PreparedStatement psCity = whConn.prepareStatement(
                    "INSERT IGNORE INTO city_dim(city_name, country, timezone) VALUES(?, ?, ?)"
            );
            PreparedStatement psDate = whConn.prepareStatement(
                    "INSERT IGNORE INTO date_dim(full_date, year, month, day) VALUES(?, YEAR(?), MONTH(?), DAY(?))"
            );
            PreparedStatement psFact = whConn.prepareStatement(
                    "INSERT INTO fact_weather(city_id, date_id, max_temp, min_temp, avg_temp, avg_humidity, maxwind_kph, uv, rain_chance, condition_text) " +
                            "VALUES ((SELECT id FROM city_dim WHERE city_name=? AND country=?), " +
                            "(SELECT id FROM date_dim WHERE full_date=?), ?, ?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                String city = rs.getString("city_name");
                String country = rs.getString("country");
                String timezone = rs.getString("timezone");
                Date fullDate = rs.getDate("full_date");

                psCity.setString(1, city);
                psCity.setString(2, country);
                psCity.setString(3, timezone);
                psCity.executeUpdate();

                psDate.setDate(1, fullDate);
                psDate.setDate(2, fullDate);
                psDate.setDate(3, fullDate);
                psDate.setDate(4, fullDate);
                psDate.executeUpdate();

                psFact.setString(1, city);
                psFact.setString(2, country);
                psFact.setDate(3, fullDate);
                psFact.setDouble(4, rs.getDouble("max_temp"));
                psFact.setDouble(5, rs.getDouble("min_temp"));
                psFact.setDouble(6, rs.getDouble("avg_temp"));
                psFact.setDouble(7, rs.getDouble("avg_humidity"));
                psFact.setDouble(8, rs.getDouble("maxwind_kph"));
                psFact.setDouble(9, rs.getDouble("uv"));
                psFact.setDouble(10, rs.getDouble("rain_chance"));
                psFact.setString(11, rs.getString("condition_text"));
                psFact.executeUpdate();
            }

            System.out.println("Dữ liệu đã được ETL từ Staging sang Warehouse thành công!");
            stgConn.close();
            whConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final String STEP_NAME = "LOAD TO WH";
        ConfigManager config = new ConfigManager("config.xml");
        ETLRunner.runAndLog(
                STEP_NAME,
                () -> new LoadToWH(config).run() //
        );
    }
}
