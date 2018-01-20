#!/usr/bin/env python
# -*- coding: utf-8 -*
from multiprocessing import Process, Lock, Pipe
import multiprocessing, argparse, time, sys, os, signal, traceback, socket, sched, atexit
import paho.mqtt.client as mqtt  # pip install paho-mqtt
import broadlink  # pip install broadlink
HAVE_TLS = True
try:
    import ssl
except ImportError:
    HAVE_TLS = False

dirname = os.path.dirname(os.path.abspath(__file__)) + '/'
CONFIG = os.getenv('BROADLINKMQTTCONFIG', dirname + 'broadlink-thermostat.conf')

class Config(object):
    def __init__(self, filename=CONFIG):
        self.config = {}
        self.config['ca_certs']     = None
        self.config['tls_version']  = None
        self.config['certfile']     = None
        self.config['keyfile']      = None
        self.config['tls_insecure'] = False
        self.config['tls']          = False
        execfile(filename, self.config)

        if HAVE_TLS == False:
            logging.error("TLS parameters set but no TLS available (SSL)")
            sys.exit(2)

        if self.config.get('ca_certs') is not None:
            self.config['tls'] = True

        if self.config.get('tls_version') is not None:
            if self.config.get('tls_version') == 'tlsv1':
                self.config['tls_version'] = ssl.PROTOCOL_TLSv1
            if self.config.get('tls_version') == 'tlsv1.2':
                # TLS v1.2 is available starting from python 2.7.9 and requires openssl version 1.0.1+.
                if sys.version_info >= (2,7,9):
                    self.config['tls_version'] = ssl.PROTOCOL_TLSv1_2
                else:
                    logging.error("TLS version 1.2 not available but 'tlsv1.2' is set.")
            	    sys.exit(2)
            if self.config.get('tls_version') == 'sslv3':
                self.config['tls_version'] = ssl.PROTOCOL_SSLv3

    def get(self, key, default='special empty value'):
        v = self.config.get(key, default)
        if v == 'special empty value':
            logging.error("Configuration parameter '%s' should be specified" % key)
            sys.exit(2)
        return v
        
def unhandeledException(e):
    trace = open('/tmp/transfer-unhandled-exception.log', 'w+')
    traceback.print_exc(None, trace)
    trace.close()

class ReadDevice(Process):
    def __init__(self, pipe, divicemac, device, conf, mqttc):
        super(ReadDevice, self).__init__()
        self.pipe         = pipe
        self.divicemac    = divicemac
        self.device       = device
        self.conf         = conf
        self.mqttc        = mqttc

    def run(self):
        print('PID child %d' % os.getpid())
        #topic = self.conf.get('mqtt_topic_prefix', '/broadlink') + '/'+self.divicemac+'/cmd/#'
        try:
            if self.device.auth():
                #print 'subscribe ' + topic
                #self.mqttc.subscribe(topic, qos=self.conf.get('mqtt_qos', 0))
                self.run = True
                print self.device.type
            else:
                self.run = False
            while self.run:
                try:
                    if self.pipe.poll(self.conf.get('loop_time', 60)):
                        result = self.pipe.recv()
                        if type(result) == tuple and len(result) == 2:
                            (cmd, opts) = result
                            if cmd=='set_temp' and float(opts)>0:
                                self.device.set_temp(float(opts))
                            elif cmd=='switch_to_auto':
                                self.device.switch_to_auto()
                            elif cmd=='switch_to_manual':
                                self.device.switch_to_manual()
                        else:
                            if result == 'STOP':
                                self.shutdown()
                                return
                    else:
                        try:
                            data = self.device.get_full_status()
                        except socket.timeout:
                            self.run = False
                            return
                        except Exception, e:
                            unhandeledException(e)
                            self.run = False
                            return
                        for key in data:
                            if type(data[key]).__name__ == 'list':
                                pass
                            else:
                                if key == 'room_temp':
                                    print "  {} {} {}".format(self.divicemac, key, data[key])
                                self.mqttc.publish('%s/%s/%s'%(self.conf.get('mqtt_topic_prefix', '/broadlink'), self.divicemac, key), data[key], qos=self.conf.get('mqtt_qos', 0), retain=self.conf.get('mqtt_retain', False))
                except Exception, e:
                    unhandeledException(e)
                    return
            return

        except KeyboardInterrupt:
            return

        except Exception, e:
            unhandeledException(e)
            return

def main():
    try:
        conf = Config()
    except Exception, e:
        print "Cannot load configuration from file %s: %s" % (CONFIG, str(e))
        sys.exit(2)

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
                        print 'send to pipe %s' % ID
                        pipe.send((command, msg.payload))
            except:
                print "Unexpected error:", sys.exc_info()[0]
                raise

    def on_disconnect(client, empty, rc):
        time.sleep(10)
        
    def on_kill(mqttc, jobs):
        mqttc.loop_stop()
        for j in jobs:
            j.join()
     
    mqttc = mqtt.Client(conf.get('mqtt_clientid', 'broadlink-%s' % os.getpid()), clean_session=conf.get('mqtt_clean_session', False), userdata=pipes)
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect
    mqttc.will_set('/broadlink', payload="Disconnect", qos=conf.get('mqtt_qos', 0), retain=False)
    mqttc.reconnect_delay_set(min_delay=3, max_delay=30)
    if conf.get('tls') == True:
        mqttc.tls_set(conf.get('ca_certs'), conf.get('certfile'), conf.get('keyfile'), tls_version=conf.get('tls_version'), ciphers=None)
    if conf.get('tls_insecure'):
        mqttc.tls_insecure_set(True)
    mqttc.username_pw_set(conf.get('mqtt_username'), conf.get('mqtt_password'))
    mqttc.connect(conf.get('mqtt_broker', 'localhost'), int(conf.get('mqtt_port', '1883')), 60)
    mqttc.subscribe(conf.get('mqtt_topic_prefix', '/broadlink') + '/+/cmd/#', qos=conf.get('mqtt_qos', 0))
    mqttc.loop_start()

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
            print "broadlink discover"
            devices = broadlink.discover(timeout=conf.get('lookup_timeout', 5))
            for device in devices:
                divicemac = ''.join(format(x, '02x') for x in device.mac)
                if divicemac not in founddevices.values():
                    transportPipe, MasterPipe = Pipe()
                    pipes.append((divicemac, MasterPipe))

                    print "found: {} {}".format(device.host[0], ''.join(format(x, '02x') for x in device.mac))
                    p = ReadDevice(transportPipe, divicemac, device, conf, mqttc)
                    jobs.append(p)
                    p.start()
                    founddevices[p.pid] = divicemac
                    time.sleep(2)
            mqttc.user_data_set(pipes)
            time.sleep(conf.get('rediscover_time', 600))
        except KeyboardInterrupt:
            run = False
        except Exception, e:
            run = False
            unhandeledException(e)
        except SignalException_SIGKILL:
            run = False

    mqttc.loop_stop()
    for j in jobs:
        j.join()
    return

main()