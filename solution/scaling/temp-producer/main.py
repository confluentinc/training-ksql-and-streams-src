import random
import time
import json

from confluent_kafka import Producer

topic = "temperature-readings"
stations = ["S-01", "S-02", "S-03", "S-04", "S-05", "S-06", "S-07", "S-08", "S-09", "S-10"]
tempAverage = [10, 15, 8, 23, 7, 2, 22, 30, -3, 13]
lastTemperature = [10, 15, 8, 23, 7, 2, 22, 30, -3, 13]

p = Producer({'bootstrap.servers': 'kafka:9092'})

def delivery_report(err, msg):
    if err:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

while True:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    stationIndex = random.randint(0, 9)
    station = stations[stationIndex]
    temperature = lastTemperature[stationIndex]
    average = tempAverage[stationIndex]
    rand = random.randint(0, 9)
    if rand < 4:         # 40% chance that temperature stays the same
        delta = 0
    elif rand < 8:    # 40% chance that temperature comes closer to the average
        delta = 1
    else:
        delta = -1
    if temperature > average:
        delta = -delta
    temperature += delta
    lastTemperature[stationIndex] = temperature 

    msg_value = json.dumps({
        "station": station,
        "temperature": temperature
    })

    print(station + ", " + msg_value)   #str(temperature))
    p.produce(topic, key=station, value=msg_value)     #, callback=delivery_report)

    time.sleep(0.1)     # sleep 100 ms

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()