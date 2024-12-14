run:
	clear
	# python3 ./extract_coordinates.py
	python3 ./main.py
	# python3 eda.py
	# python3 ./population.py
	# python3 ./viz.py && open ./index.html
	# python3 ./utils.py
	# python3 ./post_process.py
	# python3 ./google_api.py

# Download population datasets
dataset:
	wget https://data.humdata.org/dataset/191b04c5-3dc7-4c2a-8e00-9c0bdfdfbf9d/resource/fade8620-0935-4d26-b0c6-15515dd4bf8b/download/vnm_general_2020_geotiff.zip -O ./datasets/population/ && unzip ./datasets/population/vnm_general_2020_geotiff.zip -d ./datasets/population/
	wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2025_GLOBE_R2023A_4326_3ss/V1-0/tiles/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R7_C29.zip -O ./datasets/population/ && unzip ./datasets/population/GHS_POP_E2025_GLOBE_R2023A_4326_3ss_V1_0_R7_C29.zip -d ./datasets/population/

geo:
	microsoft-edge https://geojson.io/#map=12.01/21.0197/105.84102
