# Sidewalk Data Streamer
Stream data from gps, imu, dmi instruments using Kafka and Spark Streaming and save to Cassandra.<br>
Thanks to Yannael whose repository: https://github.com/Yannael/kafka-sparkstreaming-cassandra is used in part in this project, with modification.

## How to use
1. Clone the repository sidewalk_data_streamer
2. Build the Docker container: `sudo docker build -t sidewalk_data_streamer .`
3. Run the image: `sudo docker run -v `pwd`:/home/guest/host -p 4040:4040 -p 8888:8888 -p 23:22 -ti --privileged sidewalk_data_streamer`
4. Execute the startup script as root: `startup_script.sh`
5. Change user to `guest`: `su guest`
6. Start the streaming service
6.	Start the producers: `cd /home/guest/host/pipeline && ~/anaconda2/bin/python generate_spoof_data.py -d 2 -i dmi` where the `-d` option sets the delay between rows (to mimic the actual speed the data is collected) and the `-i` option selects which instrument data to send.v
6.	Start the consumers: `cd /home/guest/host/pipeline && ~/anaconda2/bin/python consume_spoof_data.py -i dmi` where the `-i` option selects which instrument data to receive data from.
7. Connect as guest: `ssh -p 23 guest@localhost`; password is `guest`


# Running the raw side of the pipeline only
We may just use the raw only for a while until we define the aggregations better. For now this is a sure-fire way to ensure that all data is saved even at very rapid speeds. The speeds indicated by the `-d` option below reflect the actual speeds that these instruments collect data. There will be other instruments collecting data, but they will likely need to be on different containers since running them is significantly different compared to these instruments.
## Producer start
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python generate_spoof_data.py -d 0.2 -i dmi
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python generate_spoof_data.py -d 0.2 -i gps
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python generate_spoof_data.py -d 0.1 -i imu

## Consumer start
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python consume_spoof_data.py -i dmi
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python consume_spoof_data.py -i gps
* cd /home/guest/host/pipeline && ~/anaconda2/bin/python consume_spoof_data.py -i imu

