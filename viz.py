import folium
import geopandas as gpd


points = gpd.read_file("./hoan_kiem.geojson")

m = folium.Map(location=[21.019655, 105.86402], zoom_start=15)

folium.GeoJson(points).add_to(m)

m.save("index.html")
