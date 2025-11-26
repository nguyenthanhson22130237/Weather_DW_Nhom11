package vn.edu.hcmuaf.fit.web.controller;

// CHÚ Ý: Dùng 'jakarta' thay vì 'javax'
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import vn.edu.hcmuaf.fit.web.ControlManager.ConfigManager;
import vn.edu.hcmuaf.fit.web.dao.DashboardDAO;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@WebServlet(name = "DashboardController", urlPatterns = {"/dashboard"})
public class DashboardController extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // 1. Khởi tạo
        // Đảm bảo file config.xml nằm trong thư mục src/main/resources
        ConfigManager config = new ConfigManager("config.xml");
        DashboardDAO dao = new DashboardDAO(config);

        // 2. Lấy dữ liệu từ Warehouse (Code JDBC giữ nguyên vì là chuẩn Java SE)
        List<DashboardDAO.WeatherData> weatherList = dao.getRecentWeather();

        // 3. Chuẩn bị dữ liệu cho ChartJS
        String labels = weatherList.stream()
                .map(w -> "'" + w.cityName + "'")
                .collect(Collectors.joining(","));

        String tempValues = weatherList.stream()
                .map(w -> String.valueOf(w.avgTemp))
                .collect(Collectors.joining(","));

        // 4. Đẩy ra JSP
        req.setAttribute("weatherList", weatherList);
        req.setAttribute("chartLabels", "[" + labels + "]");
        req.setAttribute("chartData", "[" + tempValues + "]");

        req.getRequestDispatcher("/dashboard.jsp").forward(req, resp);
    }
}