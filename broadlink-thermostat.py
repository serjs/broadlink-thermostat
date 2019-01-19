#!/usr/bin/env python
# -*- coding: utf-8 -*
from multiprocessing import Process, Lock, Pipe
import multiprocessing, argparse, time, datetime, sys, os, signal, traceback, socket, sched, atexit
import paho.mqtt.client as mqtt  # pip install paho-mqtt
import broadlink  # pip install broadlink
import json  # pip install json
import pytz  # pip install pytz
import logging

HAVE_TLS = True
try:
    import ssl
except ImportError:
    HAVE_TLS = False

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

config = {
    # MQTT
    'mqtt_broker': os.getenv('MQTT_BROKER', '127.0.0.1'),
    'mqtt_port': int(os.getenv('MQTT_PORT', 1883)),
    'mqtt_clientid': os.getenv('MQTT_CLIENT_ID', 'broadlink'),
    'mqtt_qos': os.getenv('MQTT_QOS', 2),
    'mqttname': os.getenv('MQTT_NAME', 'mqtt'),
    'mqtt_username': os.getenv('MQTT_USER', ''),
    'mqtt_password': os.getenv('MQTT_PASS', ''),
    'mqtt_topic_prefix': os.getenv('MQTT_TOPIC_PREFIX', '/broadlink'),
    'mqtt_retain': os.getenv('MQTT_RETAIN', False),
    'ca_certs': os.getenv('MQTT_CA_CERTS', None),
    'tls_version': os.getenv('MQTT_TLS_VERSION', None),
    'certificate': os.getenv('MQTT_CERTIFICATE', None),
    'keyfile': os.getenv('MQTT_KEYFILE', None),
    'tls_insecure': os.getenv('MQTT_INSECURE', False),
    'tls': os.getenv('MQTT_TLS', False),
    'mqtt_clean_session': os.getenv('MQTT_CLEAN_SESSION', False),
    # Broadlink
    'time_zone': os.getenv('BROADLINK_TIMEZONE', ''),
    'remote_lock': os.getenv('BROADLINK_LOCK', 0),
    'loop_mode': os.getenv('BROADLINK_LOOP_MODE', 0),
    'loop_time': os.getenv('BROADLINK_LOOP_TIME', 10),
    'auto_mode': os.getenv('BROADLINK_AUTO_MODE', 0),
    'rediscover_time': os.getenv('BROADLINK_REDISCOVER_TIME', 30),
    'lookup_timeout': os.getenv('BROADLINK_TIMEOUT', 30),
}

if HAVE_TLS == False:
    logging.error("TLS parameters set but no TLS available (SSL)")
    sys.exit(2)

if config['ca_certs'] is not None:
    config['tls'] = True

if config['tls_version'] is not None:
    if config['tls_version'] == 'tlsv1':
        config['tls_version'] = ssl.PROTOCOL_TLSv1
    if config['tls_version'] == 'tlsv1.2':
        # TLS v1.2 is available starting from python 2.7.9 and requires openssl version 1.0.1+.
        if sys.version_info >= (2,7,9):
            config['tls_version'] = ssl.PROTOCOL_TLSv1_2
        else:
            logging.error("TLS version 1.2 not available but 'tlsv1.2' is set.")
            sys.exit(2)
    if config['tls_version'] == 'sslv3':
        config['tls_version'] = ssl.PROTOCOL_SSLv23

def unhandeledException(e):
    logging.exception(e)

class ReadDevice(Process):
    def __init__(self, pipe, divicemac, device):
        super(ReadDevice, self).__init__()
        self.pipe         = pipe
        self.divicemac    = divicemac
        self.device       = device

    def run(self):
        logging.info('PID child %d' % os.getpid())
        mqttc= mqtt.Client(config['mqtt_clientid']+'-%s-%s' % (self.divicemac,os.getpid()), clean_session=config['mqtt_clean_session'])
        mqttc.will_set('%s/%s'%(config['mqtt_topic_prefix'], self.divicemac), payload="Disconnect", qos=config['mqtt_qos'], retain=False)
        mqttc.reconnect_delay_set(min_delay=3, max_delay=30)
        if config['tls'] == True:
            mqttc.tls_set(config['ca_certs'], config['certfile'], config['keyfile'], tls_version=config['tls_version'], ciphers=None)
        if config['tls_insecure']:
            mqttc.tls_insecure_set(True)
        mqttc.username_pw_set(config['mqtt_username'], config['mqtt_password'])
        mqttc.connect(config['mqtt_broker'], config['mqtt_port'], 60)
        mqttc.loop_start()
        try:
            if self.device.auth():
                self.run = True
                logging.info(self.device.type)
                timezone = pytz.timezone(config['time_zone'])
                now=datetime.datetime.now(timezone)
                # set device time
                self.device.set_time(now.hour, now.minute, now.second, now.weekday()+1)
                logging.info('set time %d:%d:%d %d' % (now.hour, now.minute, now.second, now.weekday()+1))
                # set auto_mode = 0, loop_mode = 0 ("12345,67")
                self.device.set_mode(config['auto_mode'], config['loop_mode'])
                # set device on, remote_lock off
                self.device.set_power(1, config['remote_lock'])
            else:
                self.run = False
            while self.run:
                try:
                    if self.pipe.poll(config['loop_time']):
                        result = self.pipe.recv()
                        if type(result) == tuple and len(result) == 2:
                            (cmd, opts) = result
                            if cmd=='set_temp' and float(opts)>0:
                                self.device.set_temp(float(opts))
                            elif cmd=='set_mode':
                                self.device.set_mode(0 if int(opts) == 0 else 1, config['loop_mode'])
                            elif cmd=='set_power':
                                self.device.set_power(0 if int(opts) == 0 else 1, config['remote_lock'])
                            elif cmd=='switch_to_auto':
                                self.device.switch_to_auto()
                            elif cmd=='switch_to_manual':
                                self.device.switch_to_manual()
                            elif cmd=='set_schedule':
                                try:
                                    schedule=json.loads(opts)
                                    self.device.set_schedule(schedule[0],schedule[1])
                                except Exception as e:
                                    pass
                        else:
                            if result == 'STOP':
                                self.shutdown()
                                mqttc.loop_stop()
                                return
                    else:
                        try:
                            data = self.device.get_full_status()
                        except socket.timeout:
                            mqttc.loop_stop()
                            return
                        except Exception as e:
                            unhandeledException(e)
                            mqttc.loop_stop()
                            return
                        for key in data:
                            if type(data[key]).__name__ == 'list':
                                mqttc.publish('%s/%s/%s'%(config['mqtt_topic_prefix'], self.divicemac, key), json.dumps(data[key]), qos=config['mqtt_qos'], retain=config['mqtt_retain'])
                                pass
                            else:
                                if key == 'room_temp':
                                    logging.info("  {} {} {}".format(self.divicemac, key, data[key]))
                                mqttc.publish('%s/%s/%s'%(config['mqtt_topic_prefix'], self.divicemac, key), data[key], qos=config['mqtt_qos'], retain=config['mqtt_retain'])
                        mqttc.publish('%s/%s/%s'%(config['mqtt_topic_prefix'], self.divicemac, 'schedule'), json.dumps([data['weekday'],data['weekend']]), qos=config['mqtt_qos'], retain=config['mqtt_retain'])
                except Exception as e:
                    unhandeledException(e)
                    mqttc.loop_stop()
                    return
            mqttc.loop_stop()
            return

        except KeyboardInterrupt:
            mqttc.loop_stop()
            return

        except Exception as e:
            unhandeledException(e)
            mqttc.loop_stop()
            return

def main():
    jobs = []
    pipes = []
    founddevices = {}

    def on_message(client, pipes, msg):
        cmd = msg.topic.split('/')
        devicemac = cmd[2]
        command = cmd[4]
        if cmd[3] == 'cmd':
            try:
                for (ID, pipe) in pipes:
                    if ID==devicemac:
                        logging.info('send to pipe %s' % ID)
                        pipe.send((command, msg.payload))
            except:
                logging.info("Unexpected error:", sys.exc_info()[0])
                raise

    def on_disconnect(client, empty, rc):
        logging.info("Disconnect, reason: " + str(rc))
        logging.info("Disconnect, reason: " + str(client))
        client.loop_stop()
        time.sleep(10)

    def on_kill(mqttc, jobs):
        mqttc.loop_stop()
        for j in jobs:
            j.join()

    def on_connect(client, userdata, flags, rc):
        client.publish(config['mqtt_topic_prefix'], 'Connect')
        logging.info("Connect, reason: " + str(rc))

    mqttc = mqtt.Client(config['mqtt_clientid']+'-%s'%os.getpid(), clean_session=config['mqtt_clean_session'], userdata=pipes)
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.will_set(config['mqtt_topic_prefix'], payload="Disconnect", qos=config['mqtt_qos'], retain=False)
    mqttc.reconnect_delay_set(min_delay=3, max_delay=30)
    if config['tls'] == True:
        mqttc.tls_set(config['ca_certs'], config['certfile'], config['keyfile'], tls_version=config['tls_version'], ciphers=None)
    if config['tls_insecure']:
        mqttc.tls_insecure_set(True)
    mqttc.username_pw_set(config['mqtt_username'], config['mqtt_password'])
    mqttc.connect(config['mqtt_broker'], int(config['mqtt_port']), 60)
    mqttc.subscribe(config['mqtt_topic_prefix'] + '/+/cmd/#', qos=config['mqtt_qos'])

    atexit.register(on_kill, mqttc, jobs)

    run = True
    while run:
        try:
            for idx, j in enumerate(jobs):
                if not j.is_alive():
                    try:
                        j.join()
                    except:
                        pass
                    try:
                        del founddevices[j.pid]
                    except:
                        pass
                    try:
                        del jobs[idx]
                    except:
                        pass
            logging.info("broadlink discover")
            devices = broadlink.discover(timeout=config['lookup_timeout'])
            for device in devices:
                divicemac = ''.join(format(x, '02x') for x in device.mac)
                if divicemac not in founddevices.values():
                    transportPipe, MasterPipe = Pipe()
                    pipes.append((divicemac, MasterPipe))

                    logging.info("found: {} {}".format(device.host[0], ''.join(format(x, '02x') for x in device.mac)))
                    p = ReadDevice(transportPipe, divicemac, device)
                    jobs.append(p)
                    p.start()
                    founddevices[p.pid] = divicemac
                    time.sleep(2)
            mqttc.user_data_set(pipes)
            mqttc.loop_stop()
            logging.info("Reconnect")
            mqttc.loop_start()
            time.sleep(config['rediscover_time'])
        except KeyboardInterrupt:
            run = False
        except Exception as e:
            run = False
            unhandeledException(e)
        except SignalException_SIGKILL:
            run = False

    mqttc.loop_stop()
    for j in jobs:
        j.join()
    return

main()
