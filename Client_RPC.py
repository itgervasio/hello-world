import zerorpc
import getopt 
import sys


def send_SLA(sla):
    framework = zerorpc.Client()
    framework.connect("tcp://127.0.0.1:20000")
    print framework.add_sla(sla)

def send_monitoring(monitoring_data):
    print monitoring_data
    framework = zerorpc.Client()
    framework.connect("tcp://127.0.0.1:20000")
    print framework.new_monitoring_data(monitoring_data)


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], 's:m:', ['--monitoring-file=','--sla-file='])
    options = { 'SLA' : False, 'Monitoring' : False }
    for o, a in opts:
        if o in ('-m', '--monitoring-file'):
            options ['Monitoring'] = a
        if o in ('-s', '--sla-file'):
            options ['SLA'] = a
    if options ['SLA']:
        # Send data
        with open (options ['SLA'], "r") as sla_file:
            SLA = sla_file.read()
        send_SLA(SLA)
    if options ['Monitoring']:
        print 'HERE'
        with open (options ['Monitoring'], "r") as myfile:
            monitoring = myfile.read()
        send_monitoring(monitoring)
