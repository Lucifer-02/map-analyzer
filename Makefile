run:
	# clear
	# uv run ./main.py ./queries/with_ocean/ha_noi.geojson 
	uv run main.py
	# python3 eda.py
	# uv run test.py

# Download population datasets
dataset:
	wget https://data.humdata.org/dataset/191b04c5-3dc7-4c2a-8e00-9c0bdfdfbf9d/resource/fade8620-0935-4d26-b0c6-15515dd4bf8b/download/vnm_general_2020_geotiff.zip && mv vnm_general_2020_geotiff.zip ./datasets/population/ && unzip ./datasets/population/vnm_general_2020_geotiff.zip -d ./datasets/population/
	wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2025_GLOBE_R2023A_4326_3ss/V1-0/tiles/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R8_C29.zip && mv GHS*.zip ./datasets/population/ && unzip ./datasets/population/GHS_*.zip -d ./datasets/population/
	wget https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/2024/VNM/v1/100m/constrained/vnm_pop_2024_CN_100m_R2024B_v1.tif && mv vnm_pop_2024_CN_100m_R2024B_v1.tif ./datasets/population/


geo:
	firefox https://hanshack.com/geotools/gimmegeodata/

viz:
	firefox https://geojson.io/#map=12.01/21.0197/105.84102

archive:
	zip -r raw.zip datasets/raw/ -x ./datasets/raw/oss/archives/* 

sync: archive
	rclone sync raw.zip map:work --progress 

clean:
	rm raw.zip
