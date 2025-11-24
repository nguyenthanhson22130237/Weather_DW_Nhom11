package vn.edu.hcmuaf.fit.web.ControlManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ETLRunner {

    public static void runAndLog(String stepName, Runnable step) {
        System.out.println("--- Bắt đầu chạy " + stepName + " ---");

        ConfigManager config = null;
        try {
            // 1. Khởi tạo Config
            config = new ConfigManager("config.xml");
        } catch (Exception e) {
            System.err.println("!!! LỖI KHỞI TẠO CONFIG: " + e.getMessage());
            System.exit(1);
            return;
        }

        int logId = -1;

        try (Connection conn = DriverManager.getConnection(
                config.getLogUrl(),
                config.getDbUserCommon(),
                config.getDbPasswordCommon()
        )) {
            vn.edu.hcmuaf.fit.web.ControlManager.ETLLogger logger = new ETLLogger(conn);
            logId = logger.startLog(stepName); // 2. Ghi log START

            if (logId == -1) {
                throw new SQLException("Không thể ghi log khởi tạo vào database.");
            }

            // 3. Thực thi logic chính
            step.run(); // Chạy logic của Extract/Load/Transform

            // 4. Kết thúc ghi log thành công
            logger.endLog(logId, "SUCCESS");
            System.out.println("--- Bước " + stepName + " hoàn thành thành công. ---");

        } catch (SQLException e) {
            System.err.println("!!! LỖI KẾT NỐI DB LOG: " + e.getMessage());
            System.exit(1);

        } catch (Exception e) {
            // 5. Xử lý lỗi trong quá trình ETL (Ghi log FAILED)
            if (logId != -1) {
                // Thử kết nối lại DB Log để ghi FAILED
                try (Connection conn = DriverManager.getConnection(config.getLogUrl(), config.getDbUserCommon(), config.getDbPasswordCommon())) {
                    new vn.edu.hcmuaf.fit.web.ControlManager.ETLLogger(conn).endLog(logId, "FAILED");
                } catch (SQLException logEx) {
                    System.err.println("!!! LỖI QUAN TRỌNG: Ghi log FAILED thất bại. !!!");
                    logEx.printStackTrace();
                }
            }
            System.err.println("!!! Bước " + stepName + " thất bại: " + e.getMessage());
            e.printStackTrace();
            System.exit(1); // Thoát với mã lỗi
        }
    }
}