import time
import json
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
import random
import datetime
import sched
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps


CERTIFICATE = "ss_11.pem.crt"
PRIVATE_KEY = "ss_11.pem.key"

# Define ENDPOINT, TOPIC, RELATOVE DIRECTORY for CERTIFICATE AND KEYS
ENDPOINT = "a2tqpjl3eibavf-ats.iot.us-east-1.amazonaws.com"
PATH_TO_CERT = "..\\config\\certificates"
PATH_TO_PVT_KEY = "..\\config\\keys\\private"
PATH_TO_ROOT_CA = "..\\config\\ca"
TOPIC_SOIL = "iot/soil"

#Weather API details
owm = OWM('0f8b321c68552dff33eeb5625f971c39')
mgr = owm.weather_manager()
TOPIC_AIR = "iot/air"

SPRIKLER_LOCATION_LIST = list()
SENSOR_LIST = list()

# AWS class to create number of objects (devices)
class AWS():
    # Constructor that accepts client id that works as device id and file names for different devices
    # This method will obviosuly be called while creating the instance
    # It will create the MQTT client for AWS using the credentials
    # Connect operation will make sure that connection is established between the device and AWS MQTT
    def __init__(self, client, certificate, private_key):
        self.client_id = client
        self.device_id = client
        self.cert_path = PATH_TO_CERT + "\\" + certificate
        self.pvt_key_path = PATH_TO_PVT_KEY + "\\" + private_key
        self.root_path = PATH_TO_ROOT_CA + "\\" + "AmazonRootCA1.pem"
        self.myAWSIoTMQTTClient = AWSIoTPyMQTT.AWSIoTMQTTClient(self.client_id)
        self.myAWSIoTMQTTClient.configureEndpoint(ENDPOINT, 8883)
        self.myAWSIoTMQTTClient.configureCredentials(self.root_path, self.pvt_key_path, self.cert_path)
        self._connect()

    # Connect method to establish connection with AWS IoT core MQTT
    def _connect(self):
        self.myAWSIoTMQTTClient.connect()

    # This method will publish the data on MQTT 
    # Before publishing we are confiuguring message to be published on MQTT
    def publish_soil_data(self):
        sprinkler = ''
        for i in range(0, len(SENSOR_LIST)):
            if self.device_id is SENSOR_LIST[i]['soil_sensor']:
                sprinkler = SENSOR_LIST[i]['sprinkler']

        print('Begin Publish')
        for i in range (10):
            message = {}    
            value = float(random.normalvariate(99, 1.5))
            value = round(value, 1)
            timestamp = str(datetime.datetime.now())
            message['deviceid'] = self.device_id
            message['timestamp'] = timestamp
            message['sprinkler'] = sprinkler
            message['datatype'] = 'Temperature'
            message['value'] = value
            messageJson = json.dumps(message)
            self.myAWSIoTMQTTClient.publish(TOPIC_SOIL, messageJson, 1) 
            print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC_SOIL)
            time.sleep(0.1)
        print('Publish End')

    def publish_air_data(self):
        for i in range(0, len(SPRIKLER_LOCATION_LIST)):

            sprinkler = SPRIKLER_LOCATION_LIST[i]['sprinkler']
            lat = SPRIKLER_LOCATION_LIST[i]['lat']
            lon = SPRIKLER_LOCATION_LIST[i]['lon']
            # sprinker_num = "sprinkler_"+str(i+1)
            observation = mgr.one_call(lat, lon)
            w = observation.current
            timestamp = str(datetime.datetime.now())
            air_message = {'timestamp': str(timestamp), 'lat': str(lat), 'lon': str(lon), 'temperature': str(w.temperature('celsius')['temp']), 'humidity': str(w.humidity), 'sprinkler': sprinkler}
            #message['sprinkler'] = sprinker_number
            messageJson = json.dumps(air_message)
            self.myAWSIoTMQTTClient.publish(TOPIC_AIR, messageJson, 1)
            print("Published: '" + json.dumps(air_message) + "' to the topic: " + TOPIC_AIR)
            time.sleep(0.2)


    # Disconect operation for each devices
    def disconnect(self):
        self.myAWSIoTMQTTClient.disconnect()

# Main method with actual objects and method calling to publish the data in MQTT
# Again this is a minimal example that can be extended to incopporate more devices
# Also there can be different method calls as well based on the devices and their working.
if __name__ == '__main__':

    #Reading the configuration file
    f = open('sprinkler_config.json', 'r')
    config = json.loads(f.read())
    f.close()

    # Read the config file and build the SoilSensor-Sprinkler map and Lon-Lat-Sprinkler map
    sprinklers = config['sprinklers']
    for sprinkler in sprinklers:
        # print(f'type: {type(sprinkler)}')
        # print(sprinkler)

        lat = sprinkler['lat']
        lon = sprinkler['lon']

        # Map Sprinkler with location coordinates
        sprinklr_loc_map = { 'sprinkler':sprinkler['name'], 'lat':lat, 'lon':lon}
        SPRIKLER_LOCATION_LIST.append(sprinklr_loc_map)
        
        #sprinkler_name = sprinkler['name']
        #print(f'sprinkler: {sprinkler_name}, lon: {lon}, lat: {lat}')

        cert = sprinkler['certificate']
        private_key = sprinkler['private_key']
        sensors = sprinkler['soil_sensors']

        for dev_id in sensors:
            #print(f'device id: {dev_id}')
            #print(f'Certificate: {cert}')
            #print(f'priate key: {private_key}')

            # Create SOil sensor device Objects and add them to SENSOR_LIST
            sprinklr_soil_sensor_map = {"soil_sensor": dev_id, 'sprinkler':sprinkler['name']}
            SENSOR_LIST.append(sprinklr_soil_sensor_map)


    #print('SPRIKLER_LOCATION_LIST')
    for item in SPRIKLER_LOCATION_LIST:
        print(item)
    
    print('')

    #print('SENSOR_LIST')
    #publish soil data for each sensor
    # for sensor in SENSOR_LIST:
    #     sensor.publish_soil_data()

    # Main method with actual objects and method calling to publish the data in MQTT
    # Again this is a minimal example that can be extended to incopporate more devices
    # Also there can be different method calls as well based on the devices and their working.
    t0 = time.time()
    print('Publish Begin')
    
    while True:
        try:
            t1 = time.time()
            diff = t1-t0

            publish_air = False
            if diff >= 300 or diff <= 1:
                publish_air = True

            if publish_air:
                t0 = t1

                # publish air data for lat and lon of all Sprinklers
                for item in SPRIKLER_LOCATION_LIST:
                    # print(item)
                    air_sensor = AWS("air_sensor", CERTIFICATE, PRIVATE_KEY)
                    air_sensor.publish_air_data()

            else:
                for sprinklr_soil_sensor_map in SENSOR_LIST:
                    sensor_dev_id = sprinklr_soil_sensor_map['soil_sensor']
                    soil_sensor_1 = AWS(sensor_dev_id, CERTIFICATE, PRIVATE_KEY)
                    soil_sensor_1.publish_soil_data()


        except KeyboardInterrupt:
            print('Publish End')
            break