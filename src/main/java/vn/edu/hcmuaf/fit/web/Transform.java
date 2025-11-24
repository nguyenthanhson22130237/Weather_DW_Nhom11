package vn.edu.hcmuaf.fit.web;

import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.web.ControlManager.ETLRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class Transform {
    private final ConfigManager config;

    public Transform(ConfigManager config) {
        this.config = config;
    }

    public void run() {
        try{
            //  Đọc file confi
            String dbUrl = config.getStagingUrl();
            String dbUser = config.getDbUserCommon();
            String dbPass = config.getDbPasswordCommon();

            // Kết nối DB
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            Statement stmt = connection.createStatement();

            // Thực thi câu lệnh Transform
            stmt.executeUpdate("DROP TABLE IF EXISTS staging_cleaned");
            String sql = """
                CREATE TABLE staging_cleaned AS
                SELECT
                    city_name,
                    country,
                    timezone,
                    STR_TO_DATE(full_date, '%Y-%m-%d') AS full_date,
                    CAST(max_temp AS DOUBLE) AS max_temp,
                    CAST(min_temp AS DOUBLE) AS min_temp,
                    CAST(avg_temp AS DOUBLE) AS avg_temp,
                    CAST(avg_humidity AS DOUBLE) AS avg_humidity,
                    CAST(maxwind_kph AS DOUBLE) AS maxwind_kph,
                    CAST(uv AS DOUBLE) AS uv,
                    CAST(rain_chance AS DOUBLE) AS rain_chance,
                    condition_text
                FROM staging_table;
            """;
            stmt.executeUpdate(sql);

            connection.close();
            System.out.println("Transform hoàn tất, dữ liệu staging_cleaned đã sẵn sàng!");

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    // Chạy thủ công
    public static void main(String[] args) {
        final String STEP_NAME = "TRANSFORM";
        ConfigManager config = new ConfigManager("config.xml");
        ETLRunner.runAndLog(
                STEP_NAME,
                () -> new Transform(config).run()
        );
    }
}
