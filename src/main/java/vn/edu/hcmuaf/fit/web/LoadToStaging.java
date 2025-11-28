package vn.edu.hcmuaf.fit.web;

import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.web.ControlManager.ETLRunner;

import java.sql.*;

public class LoadToStaging {

    // (1) Load file JAR -> JVM đã nạp LoadToStaging.jar
    // (2) JVM gọi main()
    public static void main(String[] args) {

        // (3) Khởi tạo STEP_NAME
        final String STEP_NAME = "LOAD TO STAGING";

        // (4) Gọi ETLRunner.runAndLog để quản lý log + chạy ETL
        ETLRunner.runAndLog(STEP_NAME, () -> {
            Connection connection = null;
            try {

                // (11) LoadToStaging đọc config của DB staging
                ConfigManager config = new ConfigManager("config.xml");
                String dbUrl = config.getStagingUrl();
                String dbUser = config.getDbUserCommon();
                String dbPass = config.getDbPasswordCommon();

                // (12) Kết nối database weather_staging
                connection = DriverManager.getConnection(dbUrl, dbUser, dbPass);
                Statement stmt = connection.createStatement();

                // (14) TRUNCATE bảng staging_table
                stmt.execute("TRUNCATE TABLE staging_table");

                // (15) Chuẩn bị câu lệnh LOAD CSV
                String csvFilePath = "weather_raw.csv";
                String sql = "LOAD DATA LOCAL INFILE '" + csvFilePath + "' INTO TABLE staging_table "
                        + "FIELDS TERMINATED BY ',' "
                        + "LINES TERMINATED BY '\\n' "
                        + "IGNORE 1 LINES";

                // (16) Load weather_raw.csv vào staging_table
                stmt.execute(sql);

                // (19) Thông báo "Dữ liệu đã load vào staging_table thành công từ CSV!"
                connection.close();

                System.out.println("Dữ liệu đã load vào staging_table thành công từ CSV!");

            } catch (Exception e) {
                e.printStackTrace();
                try {
                    // (18) Đóng kết nối DB nếu có
                    if (connection != null && !connection.isClosed()) {
                        connection.close();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }
}

