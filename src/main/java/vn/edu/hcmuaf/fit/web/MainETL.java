package vn.edu.hcmuaf.fit.web;
import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;

public class MainETL {
    public static void main(String[] args) {
        ConfigManager config = new ConfigManager("config.xml");

        System.out.println("=== 1. EXTRACT ===");
        new WeatherExtract(config).run();

        System.out.println("=== 2. LOAD STAGING ===");
        // Lưu ý: LoadToStaging trong code của bạn đang viết logic trong main,
        // cần sửa lại chút để gọi được từ đây, hoặc chạy tay từng cái như trên là chắc nhất.

        System.out.println("=== 3. TRANSFORM ===");
        new Transform(config).run();

        System.out.println("=== 4. LOAD WAREHOUSE ===");
        new LoadToWH(config).run();

        System.out.println("=== XONG! HÃY REFRESH DASHBOARD ===");
    }
}