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

 
# sidewalk_data_streamer
