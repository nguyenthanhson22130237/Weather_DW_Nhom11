package vn.edu.hcmuaf.fit.web;

import org.json.*;
import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.web.ControlManager.ETLRunner;

import java.io.*;
import java.net.*;

public class WeatherExtract {
    private final ConfigManager config;

    public WeatherExtract(ConfigManager config) {
        // Config được load sẵn từ ETLRunner bước (5 → 7)
        this.config = config;
    }

    /**
     * Đây là nội dung bước (11 → 15) trong flow:
     * - Call API → parse JSON → ghi CSV → throw nếu lỗi
     */
    public void run() {
        try {
            // (11) Lấy thông số từ config.xml
            String apiKey = config.getApiKey();
            String apiUrlBase = config.getApiUrl();
            String[] cities = config.getCities();
            int days = config.getDays();
            String aqi = config.getAqi();
            String alerts = config.getAlerts();

            // (12) Tạo file CSV và ghi header
            FileWriter csv = new FileWriter("weather_raw.csv");
            csv.append("city_name,country,tz_id,date,maxtemp_c,mintemp_c,avgtemp_c,avghumidity,maxwind_kph,uv,rain_chance,condition_text\n");

            // (14) Loop: xử lý từng City
            for (String city : cities) {
                // (14.2.2) Build URL + gọi HTTP GET
                String fullUrl = String.format("%s?key=%s&q=%s&days=%d&aqi=%s&alerts=%s",
                        apiUrlBase, apiKey, city, days, aqi, alerts);

                HttpURLConnection http = (HttpURLConnection) new URL(fullUrl).openConnection();
                http.setRequestMethod("GET");

                // (14.2.3) Đọc response API
                BufferedReader br = new BufferedReader(new InputStreamReader(http.getInputStream()));
                StringBuilder res = new StringBuilder();
                String l;
                while ((l = br.readLine()) != null) res.append(l);
                br.close();

                // (14.2.4) Parse JSON
                JSONObject json = new JSONObject(res.toString());
                JSONObject loc = json.getJSONObject("location");
                JSONArray forecast = json.getJSONObject("forecast").getJSONArray("forecastday");

                // (14.2.5) Ghi từng ngày forecast vào CSV
                for (int i = 0; i < forecast.length(); i++) {
                    JSONObject f = forecast.getJSONObject(i);
                    JSONObject d = f.getJSONObject("day");

                    csv.append(String.format(
                            "%s,%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s\n",
                            loc.getString("name"),
                            loc.optString("country"),
                            loc.optString("tz_id"),
                            f.getString("date"),
                            d.getDouble("maxtemp_c"),
                            d.getDouble("mintemp_c"),
                            d.getDouble("avgtemp_c"),
                            d.getDouble("avghumidity"),
                            d.optDouble("maxwind_kph"),
                            d.optDouble("uv"),
                            d.optDouble("daily_chance_of_rain"),
                            d.getJSONObject("condition").getString("text").replace(",", " ")
                    ));
                }
            }

            // (15) Đóng CSV file khi ghi xong toàn bộ
            csv.close();
            System.out.println("Extract thành công -> weather_raw.csv");

        } catch (Exception e) {
            // (13.1 hoặc 14.2.3.1) Nếu có bất kỳ lỗi nào → throw ra ngoài để Runner catch tại (16)
            throw new RuntimeException("Extract lỗi: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        // (1,2,3) Khởi tạo STEP_NAME và load config ban đầu
        final String STEP_NAME = "EXTRACT";
        ConfigManager config = new ConfigManager("config.xml");

        // (4) Gọi Runner để quản lý log + chạy extract
        ETLRunner.runAndLog(
                STEP_NAME,
                () -> new WeatherExtract(config).run() // truyền Runnable cho bước extract
        );
    }
}
