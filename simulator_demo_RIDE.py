'''
Created on Jun. 01, 2019
Mosaik script to initialize, interconnect and manage simulators

@file    simulator_demo.py
@author  Evandro de Souza, Amrinder S. Grewal
@date    2019.06.01  
@version 0.1
@company University of Alberta - Computing Science

Adapted and updated by Meaghan White in 2024.
'''
import os
import sys
import csv
import mosaik
import mosaik.util
import argparse
from datetime import datetime
from pathlib import Path
import time

import numpy as np

# Add OpenDSS directory to sys.path
open_dss_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../OpenDSS'))
if open_dss_path not in sys.path:
    sys.path.insert(0, open_dss_path)

#--- Performance test for Mosaik 2 vs Mosaik 3
Mosaik_2 = False

#--- Base Directory
BASE_DIR = os.getcwd()
BASE_DIR = str((Path(BASE_DIR)).parent) + "/"

#--- OpenDSS warp scripts directory
DSS_EXE_PATH = BASE_DIR + 'SmartGridMain/'

# IEEE33
TOPO_RPATH_FILE = 'IEEE33/outfile.dss'
NWL_RPATH_FILE  = 'IEEE33/IEEE33_NodeWithLoadFull.csv'
ILPQ_RPATH_FILE = 'IEEE33/IEEE33_InelasticLoadPQ.csv'
DEVS_RPATH_FILE = 'IEEE33/IEEE33_Devices_RIDE.csv'

#--- NS3 executables and library directory
NS3_EXE_PATH = BASE_DIR + 'NS3Mosaik'
NS3_LIB_PATH = BASE_DIR + 'ns-allinone-3.33/ns-3.33/build/lib'

# IEEE33
JSON_RPATH_FILE = DSS_EXE_PATH + 'IEEE33/gen_nodes.json'

#--- Simulators configuration
if Mosaik_2:
    SIM_CONFIG = {
        'Collector': {
            'python': 'simulator_collector_2:Collector',
        },
        'ControlSim': {
            'python': 'simulator_controltap_2:ControlSim',       
        },
        'PFlowSim': {
            'python': 'simulator_pflow_2:PFlowSim',
        },
        'PktNetSim': {
            'cmd': NS3_EXE_PATH + '/NS3MosaikSim %(addr)s --enablePcap',
            'cwd': Path( os.path.abspath( os.path.dirname( NS3_EXE_PATH ) ) ),
            'env': {
                    'LD_LIBRARY_PATH': NS3_LIB_PATH,
                    'NS_LOG': "SmartgridNs3Main=all",
            }
        },
    }
else:
    SIM_CONFIG = {
        'Collector': {
            'python': 'simulator_collector_3:Collector',
        },
        'ControlSim': {
            'python': 'simulator_controltap_3:ControlSim',       
        },
        'PFlowSim': {
            'python': 'simulator_pflow_3:PFlowSim',
        },
        'PktNetSim': {
            'cmd': NS3_EXE_PATH + '/NS3MosaikSim %(addr)s',
            'cwd': Path( os.path.abspath( os.path.dirname( NS3_EXE_PATH ) ) ),
            'env': {
                    'LD_LIBRARY_PATH': NS3_LIB_PATH,
                    'NS_LOG': "SmartgridNs3Main=all",
            }
        },
        'Estimator': {
            'python': 'simulator_dse:Estimator',       
        },
    }

#--- Simulation total time
END_TIME =  100	#  0.1 secs -- 1000 for 1, 10000 for 10, etc

#-- se period in ms
num_state_estimations = 10
calc_se_period = int(END_TIME / num_state_estimations)
calc_acc_period = int(calc_se_period / 10)

print("SE Period: {} \nAccumulation period: {}".format(calc_se_period, calc_acc_period))

#--- Application connection links
appconLinks = {}

#--- Sensors and actuators parameters
devParams = {}

#--- Mosaik Configuration
MOSAIK_CONFIG = {
    'execution_graph': False,
    'sim_progress': None,
    'start_timeout': 600,  # seconds
    'stop_timeout' : 10,   # seconds    
}

#--- Load Simulation devices and configurations
def readDevices(devsfile):
    
    global devParams
    
    current_directory = os.path.dirname(os.path.realpath(__file__))
    pathToFile = os.path.abspath(
        os.path.join(current_directory, devsfile)
    )
    if not os.path.isfile(pathToFile):
        print('File Actives does not exist: ' + pathToFile)
        sys.exit()
    else:
        with open(pathToFile, 'r') as csvFile:
            csvobj = csv.reader(csvFile)
            next(csvobj)
            for rows in csvobj:
                if(len(rows) == 11):
                    instance = rows[0] + "_" +  rows[1] + "-" + rows[2] \
                                + '.' + rows[3] + '.' + rows[4]
                    devParams[instance] = {}
                    devParams[instance]['device']      = rows[0]
                    devParams[instance]['src']         = rows[1]
                    devParams[instance]['dst']         = rows[2]
                    devParams[instance]['cidx']        = rows[3]
                    devParams[instance]['didx']        = rows[4]
                    devParams[instance]['period']      = rows[5]
                    devParams[instance]['error']       = rows[6]
                    devParams[instance]['cktElement']  = rows[7]
                    devParams[instance]['cktTerminal'] = rows[8]
                    devParams[instance]['cktPhase']    = rows[9]
                    devParams[instance]['cktProperty'] = rows[10] 

def readActives(actives_tap):

    global activesTap

    current_directory = os.path.dirname(os.path.realpath(__file__))
    pathToFile = os.path.abspath(
        os.path.join(current_directory, actives_tap)
    )
    if not os.path.isfile(pathToFile):
        print('File does not exist: ' + pathToFile)
        sys.exit()
    else:
        with open(pathToFile, 'r') as csvFile:
            csvobj = csv.reader(csvFile)
            activesTap = {(rows[0], rows[1], rows[2], rows[3], rows[4]) for rows in csvobj}
        csvFile.close()

def main():
    #--- Process input arguments
    parser = argparse.ArgumentParser(description='Run Smartgrid simulation')
    parser.add_argument( '--json_file', type=str, help='JSON config file', default = JSON_RPATH_FILE )    
    parser.add_argument( '--devs_file', type=str, help='devices connections file', default = DEVS_RPATH_FILE )
    parser.add_argument( '--random_seed', type=int, help='ns-3 random generator seed', default=1 )
    parser.add_argument( '--influxdb', action='store_true')
    parser.set_defaults(influxdb=False)
    parser.add_argument('--enable_events', type=int, default=0, help='0=baseline physical, 1=events/impairments physically')
    parser.add_argument('--link_delay', type=str, default="0.5ms", help='Link delay e.g. 0.5ms, 1ms, 5ms, 10ms')
    parser.add_argument('--link_error_rate', type=str, default="0.0001", help='Link error rate e.g. 0.0001 or 0.01')

    args = parser.parse_args()
    print( 'Starting simulation with args: {0}'.format( vars( args ) ) )
    

    readDevices(args.devs_file)
    world = mosaik.World( sim_config=SIM_CONFIG, mosaik_config=MOSAIK_CONFIG, debug=False )
    create_scenario( world, args )
    
    world.run( until=END_TIME )


def  create_scenario( world, args ):
    #---
    #--- Simulators configuration
    #---

    if Mosaik_2:
        pflowsim    = world.start('PFlowSim',
                              topofile = DSS_EXE_PATH + TOPO_RPATH_FILE,
                              nwlfile  = DSS_EXE_PATH + NWL_RPATH_FILE,
                              ilpqfile = DSS_EXE_PATH + ILPQ_RPATH_FILE,
                              step_size = 1,
                              loadgen_interval = 80,
                              verbose = 0)    
    else:
        pflowsim    = world.start('PFlowSim',
                              topofile = DSS_EXE_PATH + TOPO_RPATH_FILE,
                              nwlfile  = DSS_EXE_PATH + NWL_RPATH_FILE,
                              ilpqfile = DSS_EXE_PATH + ILPQ_RPATH_FILE,
                            #   loadgen_interval = 80, # IEEE13
                              loadgen_interval = 1000, # IEEE33
                              event_interval = 300, 
                              enable_events = args.enable_events, # 0 for baseline, 1 for events
                              verbose = 0)    
  

    estimator  = world.start('Estimator',
            eid_prefix = 'DSESim_',
            verbose = 0)

    pktnetsim = world.start( 'PktNetSim',
        model_name      = 'TransporterModel',
        json_file       = JSON_RPATH_FILE,
        devs_file       = DEVS_RPATH_FILE,
        linkRate        = "10Gbps",
        linkDelay       = args.link_delay,
        linkErrorRate   = args.link_error_rate,
        start_time      = 0,
        stop_time       = END_TIME,
        random_seed     = args.random_seed,
        verbose         = 0,
        tcpOrUdp        = "tcp", # transport layer protocols: tcp/udp
        # network architecture: P2P/CSMA/P2Pv6/CSMAv6 (supported backbone architectures)
        # When P2Pv6 or CSMAv6 is selected, secondary network is automatically fitted with
        # LR-WPAN and 6LoWPAN (make the distance between two nodes is set accordingly)
        network         = "P2Pv6",
        topology        = "IEEE33"  # For now only IEEE13 and IEEE33
    )

    collector   = world.start('Collector',
                    eid_prefix = 'Collector_',
                    verbose = 0,
                    out_list = False,
                    h5_save = True,
                    h5_panelname = 'Collector',
                    h5_storename ='CollectorStore.hd5')

 
    #---
    #--- Simulators Instances configuration
    #---

    # Declaring instance lists
    sensors = []
    controllers = []
    actuators = []
    transporters = []
    probers = []
    smartmeters = []
    phasors = []
    created_estimator_conn = []
    for key in devParams.keys():
        device          = devParams[key]['device']
        client          = devParams[key]['src']
        server          = devParams[key]['dst']
        control_loop    = devParams[key]['cidx']
        namespace       = devParams[key]['didx']
        #--- Sensor instances
        if (device == 'Sensor'):
            sensor_instance = device + '_' + client + '-' + server \
                                + '.' + control_loop + '.' + namespace
            created_sensor = False
            for sensor in sensors:
                if (sensor_instance == sensor.eid):
                    created_sensor = True
            if not created_sensor:
                sensors.append(pflowsim.Sensor(
                    cktTerminal = devParams[key]['cktTerminal'],
                    cktPhase = devParams[key]['cktPhase'],
                    eid =  sensor_instance,
                    step_size = devParams[key]['period'],
                    cktElement = devParams[key]['cktElement'], 
                    error = devParams[key]['error'],
                    verbose = 0
                ))
        elif (device == 'Phasor'):
            phasor_instance = device + '_' + client + '-' + server \
                                + '.' + control_loop + '.' + namespace
            created_phasor = False
            for phasor in phasors:
                if (phasor_instance == phasor.eid):
                    created_phasor = True
            if not created_phasor:
                phasors.append(pflowsim.Phasor(
                    cktTerminal = devParams[key]['cktTerminal'],
                    cktPhase = devParams[key]['cktPhase'],
                    eid = phasor_instance, 
                    step_size = devParams[key]['period'],  
                    cktElement = devParams[key]['cktElement'], 
                    error = devParams[key]['error'], 
                    verbose = 0
                ))
        elif (device == "SmartMeter"):
            smartmeter_instance = device + '_' + client + '-' + server \
                                + '.' + control_loop + '.' + namespace
            created_smartmeter = False
            for smartmeter in smartmeters:
                if (smartmeter_instance == smartmeter.eid):
                    created_smartmeter = True
            if not created_smartmeter:
                smartmeters.append(pflowsim.Smartmeter(
                    cktTerminal = devParams[key]['cktTerminal'],
                    cktPhase = devParams[key]['cktPhase'],
                    eid = smartmeter_instance, 
                    step_size = devParams[key]['period'],  
                    cktElement = devParams[key]['cktElement'], 
                    error = devParams[key]['error'], 
                    verbose = 0
                ))
        #--- Controller and Actuator instances for tap control
        elif (device == 'Actuator'):
            # Ignore namespace as one control loop has only one controller/actuator
            controller_instance = 'Control_' + client + '-' + server \
                                + '.' + control_loop
            created_control = False
            for controller in controllers:
                if (controller_instance == controller.eid):
                    created_control = True
            if not created_control:
                controllers.append(
                    controlsim.RangeControl(
                        eid      = controller_instance,
                        vset    = 2178,
                        bw      = 13.6125,
                        tdelay  = 60,
                        control_delay = devParams[key]['period']
                    ))
            created_actuator = False
            actuator_instance = 'Actuator_' + server \
                                + '.' + control_loop
            for actuator in actuators:
                if (actuator_instance == actuator.eid):
                    created_actuator = True
            if not created_actuator:
                actuators.append(pflowsim.Actuator(
                    cktTerminal = devParams[key]['cktTerminal'],
                    cktPhase = devParams[key]['cktPhase'],
                    eid = actuator_instance,
                    step_size = devParams[key]['period'],
                    cktElement = devParams[key]['cktElement'], 
                    error = devParams[key]['error'],
                    verbose = 0
                ))

        #--- Probers do not need transporters
        if (device == 'Prober'):
            prober_instance = 'Prober_' + client + '.' \
                            + control_loop + '.' + namespace
            probers.append(pflowsim.Prober(
                cktTerminal = devParams[key]['cktTerminal'],
                cktPhase = devParams[key]['cktPhase'],
                eid = prober_instance,
                step_size = devParams[key]['period'],
                cktElement = devParams[key]['cktElement'],
                error = devParams[key]['error'],
                verbose = 0
            ))
        else:
            #--- Transporter instances (Pktnet)
            #--- Does not make sense to transfer data through ns-3
            #--- to construct self-loops (source = destination)
            if client == server: continue
            created_transporter = False
            if (device == 'Actuator'):
                transporter_instance = 'Transp_' + client + '-' + server \
                                        + '.' + control_loop
            else:
                transporter_instance = 'Transp_' + client + '-' + server \
                                        + '.' + control_loop + '.' + namespace
            for transporter in transporters:
                if (transporter_instance == transporter.eid):
                    created_transporter = True            
            if not created_transporter:
                transporters.append(pktnetsim.Transporter(
                    src=client,
                    dst=server,
                    eid=transporter_instance
                ))


        
    #--- DSE instance
    dsesim = estimator.DSESim(
        idt = 1, 
        ymat_file = 'IEEE33/IEEE33_YMatrix.npy', 
        devs_file = DEVS_RPATH_FILE,
        acc_period = calc_acc_period, # what is this, should it change based on something?
        max_iter = 5, 
        threshold = 0.001,
        baseS = 100e3/3,            # single phase power base
        baseV = 12.66e3/np.sqrt(3),
        baseNode = 1,               # single phase voltage base
        basePF = 0.99,
        se_period = calc_se_period, # state estimation period in ms -- should we make this equal to the sampling frequency of the meters etc? i have messed with the sample period numbers in device file!
        pseudo_loads = 'IEEE33/loadPseudo.mat',
        se_result = 'IEEE33/wls_results.mat' # save the wls results
    )

    #--- Monitor instances
    monitor = collector.Monitor()

    # If influxdb is enabled then we store the data
    if args.influxdb:
        inflxudb    = world.start('InfluxDB',
                              url="http://192.168.1.204:8086",
                              token="FILL ME",
                              org="workspace",
                              bucket="OpenDSS"
                              )
        #--- InfluxDB instance
        inflxudb_connection = inflxudb.InfluxDB_Connection()
        #--- Sensors to influxdb
        mosaik.util.connect_many_to_one(world, sensors, inflxudb_connection, 'v', 't')
        for sensor in sensors:
            print('Connect', sensor.eid, 'to', inflxudb_connection.sid)        
        mosaik.util.connect_many_to_one(world, controllers, inflxudb_connection, 'v', 't')
        for controller in controllers:
            print('Connect', controller.eid, 'to', inflxudb_connection.sid)
        mosaik.util.connect_many_to_one(world, actuators, inflxudb_connection, 'v', 't')
        for actuator in actuators:
            print('Connect', actuator.sid, 'to', inflxudb_connection.sid)
        mosaik.util.connect_many_to_one(world, probers, inflxudb_connection, 'v', 't')
        for prober in probers:
            print('Connect', prober.sid, 'to', inflxudb_connection.sid)

    #---
    #--- Simulators interconnections
    #---
    
    for key in devParams.keys():
        device          = devParams[key]['device']
        client          = devParams[key]['src']
        server          = devParams[key]['dst']
        control_loop    = devParams[key]['cidx']
        namespace       = devParams[key]['didx']

        #--- Sensor to PktNet(Transporter)
        if (device == 'Sensor'):
            sensor_instance      = device + '_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            transporter_instance = 'Transp_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            for sensor in sensors:
                if (sensor_instance == sensor.eid):
                    for transporter in transporters:
                        if (transporter_instance == transporter.eid):
                            world.connect(sensor, transporter, 'v', 't')
                            print('Connect', sensor.eid, 'to', transporter.eid)
        
        #--- Phasor to PktNet(Transporter) to Estimator(DSESim)
        elif (device == 'Phasor'):
            phasor_instance = device + '_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            transporter_instance = 'Transp_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            #--- Does not make sense to transfer data through ns-3
            #--- to construct self-loops (source = destination)
            #--- Connect phasor directly to dsesim
            if client == server:
                for phasor in phasors:
                    if (phasor_instance == phasor.eid):
                        world.connect(phasor, dsesim, 'v', 't')
                continue

            for phasor in phasors:
                if (phasor_instance == phasor.eid):
                    for transporter in transporters:
                        if (transporter_instance == transporter.eid):
                            world.connect(phasor, transporter, 'v', 't')
                            # print("Connecting {} to {}".format(transporter_instance, phasor_instance))
                            print('Connect', phasor.eid, 'to', transporter.eid)
            if transporter_instance not in created_estimator_conn:
                for transporter in transporters:
                    if (transporter_instance == transporter.eid):
                        created_estimator_conn.append(transporter_instance) 
                        world.connect(transporter, dsesim, 'v', 't')
                        print('Connect', transporter.eid, 'to', dsesim.eid)
            
        #--- Smartmeter to PktNet(Transporter) to DSE(Estimator)
        elif (device == 'SmartMeter'):
            smartmeter_instance = device + '_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            transporter_instance = 'Transp_' + client + '-' + server \
                                    + '.' + control_loop + '.' + namespace
            for smartmeter in smartmeters:
                if (smartmeter_instance == smartmeter.eid):
                    for transporter in transporters:
                        if (transporter_instance == transporter.eid):
                            world.connect(smartmeter, transporter, 'v', 't')
                            print('Connect', smartmeter.eid, 'to', transporter.eid)
            if transporter_instance not in created_estimator_conn:
                for transporter in transporters:
                    if (transporter_instance == transporter.eid):
                        created_estimator_conn.append(transporter_instance) 
                        world.connect(transporter, dsesim, 'v', 't')
                        print('Connect', transporter.eid, 'to', dsesim.eid)

        #--- PktNet(Transporter) to Controller to PktNet(Transporter) to Actuator
        elif (device == 'Actuator'):
            controller_instance  = 'Control_' + client + '-' + server \
                                    + '.' + control_loop
            actuator_instance = 'Actuator_' + server + '.' + control_loop
            transporter_instance = 'Transp_' + client + '-' + server \
                                    + '.' + control_loop

            #--- Controller to PktNet
            for controller in controllers:
                if (controller_instance == controller.eid):
                    for transporter in transporters:
                        if (transporter_instance == transporter.eid):
                            world.connect(controller, transporter, 'v', 't')
                            print('Connect', controller.eid, 'to', transporter.eid)
        
            #--- PktNet(Transporter) to Actuator           
            for actuator in actuators:
                if (actuator_instance == actuator.eid):
                    for transporter in transporters:
                        if (transporter_instance == transporter.eid):
                            world.connect(transporter, actuator, 'v', 't',
                                time_shifted=True, initial_data={'v': [None], 't': [None]})
                            print('Connect', transporter.eid, 'to', actuator.eid)

            #--- PktNet(Transporter) to Controller
            #--- Find the transporter that needs to connect to this controller
            for controller in controllers:
                if (controller_instance == controller.eid):
                    for transporter in transporters:
                        # Find server and control loop of transporter
                        t_server = transporter.eid
                        t_control_loop = transporter.eid
                        t_server = t_server.split('_', 1)[1]
                        t_server = t_server.split('-', 1)[1]
                        t_server = t_server.split('.', 1)[0]
                        t_control_loop = t_control_loop.split('.')[1]
                        print(transporter.eid, " Server: ", t_server, " Control Loop: ", t_control_loop)
                        if (t_server == client and t_control_loop == control_loop):
                            world.connect(transporter, controller, 'v', 't',
                                time_shifted=True, initial_data={'v': [None], 't': [None]})
                            print('Connect', transporter.eid, 'to', controller.eid)


    #---
    #--- Simulators to Monitor
    #---

    #--- Sensor to Monitor
    mosaik.util.connect_many_to_one(world, sensors, monitor, 'v', 't')
    for sensor in sensors:
        print('Connect', sensor.eid, 'to', monitor.sid)

    #--- Controller to Monitor
    mosaik.util.connect_many_to_one(world, controllers, monitor, 'v', 't')
    for controller in controllers:
        print('Connect', controller.eid, 'to', monitor.sid)

    #--- Actuator to Monitor
    mosaik.util.connect_many_to_one(world, actuators, monitor, 'v', 't')
    for actuator in actuators:
        print('Connect', actuator.eid, 'to', monitor.sid)
    
    #--- Prober to Monitor
    mosaik.util.connect_many_to_one(world, probers, monitor, 'v', 't')
    for prober in probers:
        print('Connect', prober.eid, 'to', monitor.sid)

    #--- Phasor to Monitor
    # mosaik.util.connect_many_to_one(world, phasors, monitor, 'v', 't')
    mosaik.util.connect_many_to_one(world, phasors, monitor, 'v', 't', 'event_state')
    for phasor in phasors:
        print('Connect', phasor.eid, 'to', monitor.sid)

    #--- Smartmeter to Monitor
    # mosaik.util.connect_many_to_one(world, smartmeters, monitor, 'v', 't')
    # meg
    mosaik.util.connect_many_to_one(world, smartmeters, monitor, 'v', 't', 'event_state')
    for smartmeter in smartmeters:
        print('Connect', smartmeter.eid, 'to', monitor.sid)

    world.connect(dsesim, monitor, 'v', 't', time_shifted=True, initial_data={'v': [None], 't': [None]})
        

if __name__ == '__main__':
    sim_start_time = time.perf_counter()

    # Run the simulation.
    main()

    delta_sim_time = time.perf_counter() - sim_start_time
    print( 'simulation took {} seconds'.format( delta_sim_time ) )
