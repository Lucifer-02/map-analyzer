run:
	clear
	python3 ./main.py
	# python3 eda.py

# Download population datasets
dataset:
	wget https://data.humdata.org/dataset/191b04c5-3dc7-4c2a-8e00-9c0bdfdfbf9d/resource/fade8620-0935-4d26-b0c6-15515dd4bf8b/download/vnm_general_2020_geotiff.zip && mv vnm_general_2020_geotiff.zip ./datasets/population/ && unzip ./datasets/population/vnm_general_2020_geotiff.zip -d ./datasets/population/
	wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2025_GLOBE_R2023A_4326_3ss/V1-0/tiles/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R8_C29.zip && mv GHS*.zip ./datasets/population/ && unzip ./datasets/population/GHS_*.zip -d ./datasets/population/

geo:
	microsoft-edge https://hanshack.com/geotools/gimmegeodata/
	# microsoft-edge https://geojson.io/#map=12.01/21.0197/105.84102
