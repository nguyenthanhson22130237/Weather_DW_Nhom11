package vn.edu.hcmuaf.fit.web.ControlManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ETLRunner {

    public static void runAndLog(String stepName, Runnable step) {

        // (5) ETLRunner in console
        System.out.println("Bắt đầu chạy " + stepName);

        ConfigManager config = null;
        try {
            // (6) Tạo ConfigManager để đọc file config.xml
            config = new ConfigManager("config.xml");
        } catch (Exception e) {
            // (7) Thông báo "LỖI KHỞI TẠO CONFIG"
            System.err.println("!!! LỖI KHỞI TẠO CONFIG " + e.getMessage());
            System.exit(1);
        }

        int logId = -1;

        // (8) ETLRunner kết nối DB Log (etl_log)
        try (Connection conn = DriverManager.getConnection(
                config.getLogUrl(),
                config.getDbUserCommon(),
                config.getDbPasswordCommon()
        )) {
            ETLLogger logger = new ETLLogger(conn);

            if (logger.checkStepExecutedToday(stepName)) {
                System.out.println("┌──────────────────────────────────────────────────────────┐");
                System.out.println(String.format("│ THÔNG BÁO: Bước %-10s đã chạy THÀNH CÔNG hôm nay!    │", stepName));
                System.out.println("│ Hệ thống sẽ không chạy lại để tránh trùng lặp dữ liệu.   │");
                System.out.println("└──────────────────────────────────────────────────────────┘");
                return; // END workflow
            }

            // (10) Ghi log START vào etl_log, lấy logId
            logId = logger.startLog(stepName);
            if (logId == -1) {
                throw new SQLException("Không thể ghi log START vào DB.");
            }

            // Chạy bên LoadToStaging (11 - 19)
            step.run();

            // (20) Ghi log SUCCESS vào etl_log
            logger.endLog(logId, "SUCCESS", "Hoàn thành thành công");

            // (21) ETLRunner in console STEP DONE
            System.out.println("--- Bước " + stepName + " Thành công! ---");

        } catch (SQLException e) {
            // (9) Thông báo "LỖI KẾT NỐI DB LOG"
            System.err.println("!!! LỖI KẾT NỐI DB LOG: " + e.getMessage());
            System.exit(1);

        } catch (Exception e) {
            if (logId != -1) {
                try (Connection conn = DriverManager.getConnection(
                        config.getLogUrl(),
                        config.getDbUserCommon(),
                        config.getDbPasswordCommon()
                )) {
                    new ETLLogger(conn).endLog(logId, "FAILED",e.getMessage());
                    System.out.println("LOG: Đã ghi log FAILED vào Database.");
                } catch (SQLException logEx) {
                   // (13) hoặc (17) Ghi FAILED vào etl_log
                    System.err.println("!!! LỖI QUAN TRỌNG: Ghi log FAILED thất bại");
                }
            }
            e.printStackTrace();
            System.exit(1);
        }
    }
}
