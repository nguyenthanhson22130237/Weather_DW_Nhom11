<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="jakarta.tags.core" prefix="c" %>

<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <title>Weather Warehouse Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-light">

<div class="container mt-4">
    <h1 class="text-center text-primary mb-4">Dữ Liệu Kho Thời Tiết (Warehouse)</h1>

    <div class="row">
        <div class="col-md-12 mb-5">
            <div class="card shadow">
                <div class="card-header bg-white fw-bold">Biểu đồ Nhiệt độ gần đây</div>
                <div class="card-body">
                    <canvas id="tempChart" height="100"></canvas>
                </div>
            </div>
        </div>

        <div class="col-md-12">
            <div class="card shadow">
                <div class="card-header bg-success text-white">Dữ liệu chi tiết</div>
                <div class="card-body">
                    <table class="table table-hover table-bordered">
                        <thead class="table-light">
                        <tr>
                            <th>Thành Phố</th>
                            <th>Ngày</th>
                            <th>Nhiệt độ (°C)</th>
                            <th>Độ ẩm (%)</th>
                            <th>Trạng thái</th>
                        </tr>
                        </thead>
                        <tbody>
                        <c:if test="${empty weatherList}">
                            <tr><td colspan="5" class="text-center">Không có dữ liệu trong Warehouse</td></tr>
                        </c:if>

                        <c:forEach var="w" items="${weatherList}">
                            <tr>
                                <td>${w.city}</td>
                                <td>${w.date}</td>
                                <td>
                                    <span class="badge ${w.temp > 30 ? 'bg-danger' : 'bg-primary'}">
                                            ${w.temp}
                                    </span>
                                </td>
                                <td>${w.humidity}</td>
                                <td>${w.condition}</td>
                            </tr>
                        </c:forEach>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>

<script>
    // Vẽ biểu đồ
    const ctx = document.getElementById('tempChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar', // Có thể đổi thành 'line'
        data: {
            labels: ${chartLabels}, // Dữ liệu từ Servlet
            datasets: [{
                label: 'Nhiệt độ (°C)',
                data: ${chartTemps},
                backgroundColor: 'rgba(54, 162, 235, 0.6)',
                borderColor: 'rgba(54, 162, 235, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            scales: { y: { beginAtZero: true } }
        }
    });
</script>

</body>
</html>