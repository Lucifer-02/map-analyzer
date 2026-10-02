# Repository context: map-analyzer

Tài liệu này tóm tắt cấu trúc và luồng xử lý nhìn thấy trong working tree ngày 2026-09-26. Đây là repo Python xử lý dữ liệu địa lý/POI tại Việt Nam; không phải web service.

## Mục đích và luồng dữ liệu

Repo thu thập POI (địa điểm) từ các truy vấn bản đồ quanh những điểm mẫu bên trong vùng GeoJSON, lọc kết quả về trong polygon, lưu dữ liệu thô dạng Parquet, rồi có các hàm riêng để gộp dữ liệu, phân loại POI và đối chiếu với dân số raster quanh ATM/PGD.

Luồng tổng quát:

1. Đọc ranh giới từ `queries/**/*.geojson`.
2. Tạo lưới điểm trong polygon bằng `mylib.utils.find_points_in_polygon`; khoảng cách mẫu có thể được nhân với hệ số mật độ dân số.
3. Gọi engine crawler cho từng điểm và các loại POI trong `mylib.ALL_TYPES`.
4. Lọc điểm trả về nằm trong polygon, ghi file Parquet theo từng điểm vào `datasets/raw/oss/`.
5. Các hàm `summary()` và `final_result()` trong `main.py` gộp/khử trùng lặp và xuất dữ liệu POI tổng hợp.
6. Các hàm `post_process_atm()` / `post_process_pgd()` đếm POI trong bán kính 1 km và ước tính dân số từ GeoTIFF.

## Cấu trúc chính

- `main.py`: điều phối crawl, tính hệ số mật độ, gộp dữ liệu, xuất kết quả và các luồng phân tích ATM/PGD.
- `mylib/__init__.py`: định nghĩa các nhóm loại POI (`POI_GROUPS`), hợp `ALL_TYPES`, danh sách area.
- `mylib/utils.py`: xử lý GeoJSON, tạo điểm mẫu, lọc theo polygon/bán kính, khoảng cách và hình học.
- `mylib/population.py`: cắt raster theo vùng quan tâm và cộng giá trị dân số; `pop_in_radius()` dựng vùng tròn quanh điểm trước khi tính.
- `mylib/viz.py`: trực quan hóa điểm/vùng bằng Folium và Matplotlib.
- `mylib/extract_coordinates.py`: thử lấy tọa độ/link Google Maps từ truy vấn; dùng requests và Playwright.
- `mylib/post_process.py`: luồng cũ để ghép các POI xung quanh một số POI Hà Nội.
- `engines/gosom_scraper/`: wrapper gọi binary Google Maps Scraper theo hệ điều hành, đọc CSV kết quả và chuẩn hóa các cột.
- `engines/google_api/places_api.py`: wrapper tìm địa điểm gần đó qua Google Places API.
- `engines/omkarcloud_scraper/`: crawler thay thế; thêm thư mục scraper vào `sys.path` theo đường dẫn tương đối.
- `queries/`: 93 file GeoJSON theo thống kê tại thời điểm ghi tài liệu; có bộ `with_ocean/` và một số vùng/ranh giới khác.
- `datasets/population/`: bảng mật độ `V02.01.csv` và GeoTIFF dân số; `datasets/raw/oss/` chứa Parquet crawl theo điểm.
- `checklist.csv`, `TODO.md`: ghi chú/việc còn lại.

## Chạy và cấu hình

Project dùng `uv`, Python `>=3.12`; cấu hình phụ thuộc nằm trong `pyproject.toml`. Các thư viện chính gồm GeoPandas, Shapely (được dùng qua mã), Polars, Rasterio, GeoPy, Click, Playwright và Folium.

README hiện ghi lệnh:

```bash
uv run main.py <đường_dẫn_geojson>
```

Tuy nhiên, ở phiên bản code đang đọc, `main()` chạy trực tiếp lại dùng cố định `queries/with_ocean/ha_noi.geojson` và gọi `test_area_crawl2`; `cli()` nhận tham số `area` nhưng lời gọi `cli()` đang bị comment. Vì vậy lệnh README có thể không chạy theo area truyền vào như người dùng kỳ vọng. `Makefile` cũng chạy `uv run main.py` không đối số. Nên kiểm tra/đồng bộ entry point trước khi vận hành crawl hàng loạt.

Các đường dẫn dữ liệu hầu hết là tương đối với thư mục gốc repo. Crawler tạo file tạm trong thư mục engine. `main.py` ghi log vào `crawling.log`; crawler Gosom gọi binary đã được commit trong `engines/gosom_scraper/`.

## Định dạng và quy ước dữ liệu

- GeoJSON dùng thứ tự tọa độ GeoJSON `(longitude, latitude)`; `geopy.Point` trong code thường nhận `(latitude, longitude)`.
- `geojson_to_polygons()` hỗ trợ `Polygon`, `MultiPolygon` và `FeatureCollection` chỉ chứa một feature Polygon; phần xử lý hiện lấy vòng ngoài và bỏ qua holes.
- `find_points_in_polygon()` rải lưới trong bounding box rồi chỉ giữ điểm mà polygon `contains`; mặc định không thêm các đỉnh biên.
- Gosom chuẩn hóa CSV thành các cột `query`, `link`, `title`, `category`, `categories`, `latitude`, `longitude`, `address`, `complete_address`.
- Crawl qua `scrape_google_maps` trong `main.py` lọc kết quả về polygon trước khi ghi Parquet. Tên file mang dạng `<area>_<polygon_index>_<point_index>.parquet` và file tồn tại sẽ được bỏ qua.
- Tổng hợp tạo cờ `is_poi_transport`, `is_poi_ecom`, `is_poi_popu`, `is_ATM`; kết quả cuối được đổi `title` thành `name`.

## Dữ liệu lớn và đầu ra

Thư mục `datasets/raw/oss/` chứa hơn một nghìn Parquet crawl theo từng điểm (1014 file ở lần kiểm tra này). Raster GeoTIFF và các đầu ra có thể chiếm dung lượng đáng kể. Theo mã, các đầu ra tổng hợp dự kiến gồm `datasets/raw/oss/summary/*.parquet`, `datasets/results/vietnam.parquet`, `vietnam_pois.parquet`, `count_atms.parquet`, `count_pgds.parquet` và Excel tùy tác vụ. Một số thư mục/đầu vào chỉ được tham chiếu trong mã/Makefile và có thể không tồn tại trong mọi checkout.

## Điểm cần lưu ý khi phát triển

- `pyproject.toml` hiện không khai báo rõ một số import được dùng trực tiếp, gồm `click`, `map_miner`, `googlemaps`, `pandas`, `requests` và `shapely` (Shapely có thể được cài gián tiếp qua GeoPandas). Cần xác nhận môi trường cài đặt thực tế trước khi giả định cài mới chạy được.
- `main.py` chứa cả luồng crawl thử nghiệm và luồng xử lý sản xuất; nhiều hàm chọn dữ liệu/đầu ra bằng đường dẫn cố định.
- Crawler ngoài gọi dịch vụ bản đồ và có thể chịu giới hạn/chặn phía dịch vụ. Không khởi động crawl khi chỉ cần đọc hoặc sửa code.
- `Makefile` target `dataset` tải raster từ xa; target `sync` dùng `rclone` và remote tên `map`.
- Không ghi credential hoặc khóa API vào tài liệu. Nếu chỉnh sửa phần Google API, cần kiểm tra cấu hình khóa hiện hành và chuyển secret ra khỏi mã nguồn.

## Trạng thái working tree tại thời điểm ghi

Working tree đã có thay đổi chưa commit trước khi tạo tài liệu này: `main.py`, `mylib/extract_coordinates.py`, `mylib/population.py`, `mylib/post_process.py`, `mylib/utils.py`, `mylib/viz.py`; có file mới `datasets/raw/oss/read_parquet.py` và thư mục `debug/`. Không chỉnh sửa các mục đó trong quá trình ghi context. Thông tin về hành vi code ở trên mô tả working copy được kiểm tra; các diff đang có có thể làm khác với nhánh `main` đã commit.

## Kiểm tra

Tài liệu được tổng hợp bằng cách đọc mã và cấu hình; không chạy crawler, tải dataset hoặc chạy test.
