package vn.edu.hcmuaf.fit.web;

import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.web.ControlManager.ETLRunner;

import java.sql.*;

public class LoadToStaging {
    private final ConfigManager config;

    public LoadToStaging(ConfigManager config) {
        this.config = config;
    }

    public void run () {
        try {
            //  Đọc file config.xml
            String dbUrl = config.getStagingUrl();
            String dbUser = config.getDbUserCommon();
            String dbPass = config.getDbPasswordCommon();

            // Kết nối DB
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            Statement stmt = connection.createStatement();

            // Làm sạch bảng staging trước khi load
            stmt.execute("TRUNCATE TABLE staging_table");

            // Load dữ liệu vào bảng staging
            String csvFilePath = "weather_raw.csv"; // <-- đường dẫn file CSV

            String sql = "LOAD DATA LOCAL INFILE '" + csvFilePath + "' INTO TABLE staging_table " +
                    "FIELDS TERMINATED BY ',' " +
                    "LINES TERMINATED BY '\\n' " +
                    "IGNORE 1 LINES";

            stmt.execute(sql);

            connection.close();
            System.out.println("Dữ liệu đã load vào staging_table thành công từ CSV!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // Chạy thủ công
    public static void main(String[] args) {
        final String STEP_NAME = "LOAD TO STAGING";
        ConfigManager config = new ConfigManager("config.xml");
        ETLRunner.runAndLog(
                STEP_NAME,
                () -> new LoadToStaging(config).run()
        );
    }
}
