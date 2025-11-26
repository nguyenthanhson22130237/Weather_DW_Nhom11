package vn.edu.hcmuaf.fit.web.ControlManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ETLRunner {

    /**
     * (4) Được gọi từ Main: ETLRunner.runAndLog(STEP_NAME, runnable)
     * - Quản lý vòng đời log + gọi core logic extract
     */
    public static void runAndLog(String stepName, Runnable step) {

        // (4.1) In console bắt đầu kiểm tra
        System.out.println("--- Bắt đầu kiểm tra bước: " + stepName + " ---");

        ConfigManager config = null;
        int logId = -1;

        try {
            // (5, 6, 7) Khởi tạo ConfigManager & đọc config.xml & parse thông số
            config = new ConfigManager("config.xml");

            // (8, 9.1) Kết nối DB Log (etl_log)
            try (Connection conn = DriverManager.getConnection(
                    config.getLogUrl(),
                    config.getDbUserCommon(),
                    config.getDbPasswordCommon()
            )) {
                ETLLogger logger = new ETLLogger(conn); // logger thao tác với DB log

                // (9.2) Check đã chạy SUCCESS hôm nay chưa?
                if (logger.checkStepExecutedToday(stepName)) {
                    // (9.2.1 YES) → in thông báo và END LUÔN
                    System.out.println("┌──────────────────────────────────────────────────────────┐");
                    System.out.println(String.format("│ THÔNG BÁO: Bước %-10s đã chạy THÀNH CÔNG hôm nay!    │", stepName));
                    System.out.println("│ Hệ thống sẽ không chạy lại để tránh trùng lặp dữ liệu.   │");
                    System.out.println("└──────────────────────────────────────────────────────────┘");
                    return; // END workflow
                }

                // (10) Start log → status = RUNNING → lấy logId
                logId = logger.startLog(stepName);

                // (Nếu không tạo được logId → lỗi → Runner xử lý FAILED)
                if (logId == -1) {
                    throw new SQLException("Không thể khởi tạo log RUNNING.");
                }

                // (11 → 15) Chạy core business logic của bước được truyền vào
                step.run(); // gọi WeatherExtract.run()

                // (17) Ghi log kết thúc → SUCCESS + message
                logger.endLog(logId, "SUCCESS", "Hoàn thành thành công");
                System.out.println("--- Bước " + stepName + " Thành công! ---");

            } catch (SQLException e) {
                // (9.0.1) Lỗi kết nối DB log → STOP LUÔN
                System.err.println("!!! Lỗi Database Log: " + e.getMessage());
                System.exit(1);
            }

        } catch (Exception e) {
            // (16) Nếu có lỗi trong core logic → CATCH tại đây
            System.err.println("!!! Bước " + stepName + " gặp lỗi: " + e.getMessage());

            if (logId != -1 && config != null) {
                // (16.2) Kết nối lại DB Log để ghi FAILED log
                try (Connection conn = DriverManager.getConnection(
                        config.getLogUrl(),
                        config.getDbUserCommon(),
                        config.getDbPasswordCommon()
                )) {
                    ETLLogger logger = new ETLLogger(conn);
                    // (16.3) Cập nhật log → FAILED + message lỗi
                    logger.endLog(logId, "FAILED", e.getMessage());
                    System.out.println("LOG: Đã ghi log FAILED vào Database.");
                } catch (SQLException logEx) {
                    System.err.println("!!! Lỗi quan trọng: Không ghi được FAILED log.");
                }
            }

            e.printStackTrace();
            System.exit(1); // STOP chương trình khi FAILED
        }
    }
}
