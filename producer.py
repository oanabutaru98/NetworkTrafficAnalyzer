from time import sleep
from struct import *
from kafka import KafkaProducer
from scapy.all import *
from datetime import datetime
import datetime

#Connect to kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: x.encode('utf-8'))

print("Created Producer\n")

intialFileName = '../pcap-files/output.pcap'
count = 1

#The original file was spplitted. This is to move through all of them. 
while count <= 122:
    #this file is for log purposes
    logg = open("logpython.txt", "a")

    filename = intialFileName + str(count)
    logg.write("Processing " + filename + ".. ")
    count = count + 1
    
    packets = rdpcap(filename)
    #Packet iteration
    for packet in packets:
        #info gathering
        fullTime = str(packet.time)
        unixTime1 = fullTime.split(".")[0]
        milliseconds = fullTime.split(".")[1]
        unixTime = datetime.datetime.fromtimestamp(int(unixTime1))
        #print(unixTime)
        #in case there is info, it will be taken, otherwise an empty space will be set
        if IP in packet:
            ip_src = str(packet[IP].src)
            ip_dest = str(packet[IP].dst)
        else:
            ip_src = " "
            ip_dest = " "

        if TCP in packet:
            s_port = str(packet[TCP].sport)
            d_port = str(packet[TCP].dport)
        else:
            s_port = " "
            d_port = " "

        frame_size = len(packet)
        #MMsg that will be sent
        messageToConsumer = str(unixTime) + "," + str(milliseconds) + "," + ip_src + "," + ip_dest + "," + s_port + "," + d_port + "," + str(frame_size)
        #print(messageToConsumer)
        #Finally the message is sent to the topic named final
        producer.send('final', messageToConsumer)
        #sleep(5)

    #Logging purpose
    logg.write("    ... Done file \n")
    logg.close()


