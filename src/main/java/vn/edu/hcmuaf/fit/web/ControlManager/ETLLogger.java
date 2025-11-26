package vn.edu.hcmuaf.fit.web.ControlManager;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class ETLLogger {
    private final Connection conn;

    // Connection sống trong scope của Runner truyền vào
    public ETLLogger(Connection conn) {
        this.conn = conn;
    }

    /**
     * (9.2) Kiểm tra xem hôm nay đã chạy thành công bước này chưa?
     * → gọi trong ETLRunner sau khi kết nối DB Log thành công
     */
    public boolean checkStepExecutedToday(String stepName) {
        String sql = "SELECT COUNT(*) FROM etl_log WHERE step_name = ? AND status = 'SUCCESS' AND log_date = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, stepName);
            ps.setDate(2, Date.valueOf(LocalDate.now())); // (9.2.1) So sánh log_date = hôm nay

            ResultSet rs = ps.executeQuery(); // (14.2.3) hoặc JSON lỗi cũng sẽ throw → Runner catch
            if (rs.next()) {
                return rs.getInt(1) > 0; // (9.2.2) COUNT > 0 => đã chạy SUCCESS hôm nay
            }
        } catch (SQLException e) {
            e.printStackTrace(); // Runner sẽ xem như chưa chạy hôm nay
        }
        return false;
    }

    /**
     * (10) Ghi log START → status = RUNNING → lấy logId
     * → ETLRunner giữ lại logId để tracking lifecycle
     */
    public int startLog(String stepName) {
        String sql = "INSERT INTO etl_log (step_name, start_time, status, log_date) VALUES (?, ?, 'RUNNING', ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, stepName); // (10.1) step_name
            ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.now())); // (10.2) start_time
            ps.setDate(3, Date.valueOf(LocalDate.now())); // (10.3) log_date

            ps.executeUpdate(); // inserts RUNNING log

            // (10.4) Lấy khóa id vừa tạo
            ResultSet keys = ps.getGeneratedKeys();
            if (keys.next()) {
                return keys.getInt(1); // logId trả về cho ETLRunner
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1; // Runner sẽ xem là lỗi khởi tạo log RUNNING
    }

    /**
     * (17) Ghi log kết thúc → update status = SUCCESS/FAILED + message + end_time
     * → nếu có lỗi, ETLRunner cũng gọi endLog(logId, FAILED, ...)
     */
    public void endLog(int logId, String status, String message) {
        String sql = "UPDATE etl_log SET end_time = ?, status = ?, message = ? WHERE id = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now())); // (17.1) current time → end_time
            ps.setString(2, status); // (17.2) SUCCESS hoặc FAILED
            ps.setString(3, message); // (17.3) message chi tiết (completed hoặc lỗi)
            ps.setInt(4, logId); // (17.4) WHERE id = logId RUNNING

            ps.executeUpdate(); // hoàn tất update status
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
