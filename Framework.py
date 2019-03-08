import Parser.Parser as parser
import logging
import Parser.Monitoring_Parser as monitoring_parser
from Communication_Interfaces.PMS import TCP_Interface
import Z3.SLA_Constraint 
import Z3.Monitoring_Constraint
import z3
import gevent
import exceptions
import zerorpc
import datetime
from gevent import monkey
monkey.patch_all()

class SLA_Management_Framework(object):

    SCHEDULER_CLOUD = '192.168.0.1:40000'

    def __init__(self):
        logging.basicConfig(filename='log/framework.log', level=logging.INFO)
        self.slas = dict()
        self.monitoring_data = dict()
        self._start_interfaces()
        gevent.joinall([self.rpc_interface])

    def _check_guarantees(self, unsat_constraints, sla, monitoring):
        #TODO Rewrite in two methods to reduce complexity
        actions = []
        for guarantee in sla.guarantees:
            if guarantee['Complete_Metric'] == 'any':
                if unsat_constraints is not None:
                    actions.extend(self._test_conditions(guarantee, sla, monitoring))
            else:
                guarantee ['iD'] = self._associate_guarantee_term(guarantee['Complete_Metric'], sla)
                if guarantee ['iD'][0] in unsat_constraints:
                    actions.extend(self._test_conditions(guarantee, sla, monitoring))
                else:
                    logging.info( 'No Gurantees associate with this violations' )
        return actions

    def _test_conditions(self, guarantee, sla, monitoring):
        actions = []
        accessed = False
        conditions = guarantee ['if_conditions_actions']
        for condition in conditions:
            if self._test_condition (condition [0], sla, monitoring):
                accessed = True
                self._execute_actions ( condition [-1]  )
                actions.append( condition [-1]  )
                break
        if not accessed:
            if guarantee ['else_actions']:
                self._execute_actions( guarantee ['else_actions'] )
                actions.append(guarantee ['else_actions'] ) 
            else:
                logging.info( 'No actions on this Violation' )
        return actions



    def _test_condition(self, condition, sla,  monitoring):
        variable_one = condition ['variables'] [0]
        if variable_one ['Type']  == 'Metric':
            operandum_one = self._associate_guarantee_term (condition [ 'variables' ] [0] ['Complete_Metric'], sla) 
            try:
                operandum_two = float ( condition [ 'variables' ] [1] ['Value'] )
            except exceptions.ValueError:
                operandum_two = float ( condition [ 'variables' ] [1] ['Value'] )
            for k in monitoring.metrics:
                if k['Metric_Name'] == operandum_one[0]:
                    operandum_one = float (k['Value'] )
                    break
        else:
            operandum_two = self._associate_guarantee_term (condition [ 'variables' ] [1] ['Complete_Metric'], sla) 
            try:
                operandum_one = float ( condition [ 'variables' ] [0] ['Value'] )
            except exceptions.ValueError:
                operandum_one = float ( condition [ 'variables' ] [0] ['Value'] )
            for k in monitoring.metrics:
                if k['Metric_Name'] == operandum_two[0]:
                    operandum_two = float (k['Value'] )
                break
        return condition [ 'operator' ] ( operandum_one, operandum_two)

    def _associate_guarantee_term(self, guarantee_name, sla):
        guarantee_ref = guarantee_name.split(':')
        result = []
        for term in sla.constraints.values():
            #If the user specifies an ambiguos guarantee, get the smallest name (non in group)
            if self._check_part_in_term(guarantee_ref, term.split(':')):
                key = [k for k, v in sla.constraints.iteritems() if v == term]
                value = [v for k, v in sla.constraints.iteritems() if v == term]
                result.append ((key[0], value[0]))
        result_length = [ len(x[1]) for x in result ]
        if len(result) > 1:
            return result[result_length.index(min(result_length))]
        if len(result) < 1:
            raise Exception('ERROR, Guarantee METRIC NOT FOUND: ', guarantee_ref)
        else:
            return result[0] 

    def _check_part_in_term(self,  guarantee_ref, term):
        result_term = []
        for part in guarantee_ref:
            if part in term:
                result_term.append(term.index(part))
        if not all(b >= a for a, b in zip(result_term, result_term[1:])):
            return False
        elif len(result_term) == len(guarantee_ref):
            return True
        else:
            return False

    def _execute_actions(self, actions):
        for action in actions:
            logging.info('Execute Action:' + action)


    def add_single_SLA_network(self, SLA):
        logging.info('Adding new SLA to the System ' + SLA )
        sla_parser = parser.SLAC()
        parsed_SLA = sla_parser.parse(SLA)
        sla = Z3.SLA_Constraint.SLA_to_Constraint(**parsed_SLA[0]) 
        if sla.convert_SLA_to_Constraints():
            self.slas [ parsed_SLA [0] ['iD'] ] = sla
            return 'Done'
        else:
            logging.error('ERROR Could not transform SLA into constraints')
            return 'ERROR Could not transform SLA into constraints'

    def add_deploy_SLA_network(self, SLA):
        logging.info('Adding new SLA to the System ' + SLA )
        sla_parser = parser.SLAC()
        parsed_SLA = sla_parser.parse(SLA)
        sla = Z3.SLA_Constraint.SLA_to_Constraint(**parsed_SLA[0]) 
        if sla.convert_SLA_to_Constraints():
            self.slas [ parsed_SLA [0] ['iD'] ] = sla
            self._deploy_SLA(sla)
        else:
            logging.error('ERROR Could not transform SLA into constraints')

    def _deploy_SLA(self, sla):
        metrics = dict()
        for metric_id, metric_name in sla.constraints.iteritems():
            metric_group = None
            metric_ref = None
            if 'Cluster' in metric_name and len(metric_name.split(':')) > 3:
                metric_group = 'Cluster'
            elif 'Large_VM' in metric_name and len(metric_name.split(':')) > 3:
                metric_group = 'Large_VM'

            if 'RT_delay' == sla.constraints_metrics[metric_id]:
                metric_type = 'server'
            elif metric_group is not None and len(metric_name.split(':')) > 4 :
                metric_type = 'vms'
            elif metric_group is None and len(metric_name.split(':')) > 2 and 'Small_VM' not in metric_name:
                metric_type = 'server'
            else:
                metric_type = 'internal'

            if metric_group is not None:
                metric_ref = metric_name.split(':')[-2]
            metrics [metric_id] = {'Name' : sla.constraints_metrics[metric_id], 'Type' : metric_type, 'Group' : metric_group, 'Value' : None, 'Ref Machine' : metric_ref} 
            
        scheduler = zerorpc.Client()
        scheduler.connect("tcp://"+self.SCHEDULER_CLOUD)
        #TODO retrieve local ip
        scheduler.instantiate_new_service(sla.iD, sla.instantiated_groups, metrics, '127.0.0.1:20000')

    def new_monitoring_data(self, monitoring_data):
        #TODO Add ID
        print monitoring_data
        logging.info( 'New Monitoring Data' + monitoring_data)
        monitoring_Parser = monitoring_parser.Monitoring_Parser()
        parsed_monitoring_data = monitoring_Parser.parse(monitoring_data)
        parsed_monitoring_data [0] ['lm_elements'] = self.slas [parsed_monitoring_data [0] ['iD']].lm_elements
        self.monitoring_data [parsed_monitoring_data [0] ['iD']] = Z3.Monitoring_Constraint.Monitoring_to_Constraint(**parsed_monitoring_data[0])
        results = self.verify_SLA(parsed_monitoring_data[0]['iD'])
        outputfile = 'SLA_ID-'+parsed_monitoring_data[0]['iD']+'-'+str(datetime.datetime.now())
        with open('log/'+outputfile, "w") as text_file:
                text_file.write(monitoring_data)
                print 'done'
        return results

    def verify_SLA(self, iD):
        try:
            unsat_constraints = self._check_SLA(self.slas[iD], self.monitoring_data[iD])
        except Exception, e:
            logging.error('No SLA or Monitoring data found with the iD: ' + str(iD))
            logging.debug('SLAs in the framework: ' + str(self.slas))
            logging.debug('Monitoring data in the framework: ' + str(self.monitoring_data))
            return False
            #print 'SLAs in the Framework: ', self.slas
            #print 'Monitornig data in the Framework: ', self.monitoring_data
        if unsat_constraints and unsat_constraints != 'Satisfied':
            #TODO add id monitoring Data
            print unsat_constraints
            logging.info('Constraints Unsatisfied for SLA:' + iD  + ' with the Monitoring Data: ' + str([self.slas [iD].constraints[unsat_constraint] for unsat_constraint in unsat_constraints]))
            actions = self._check_guarantees(unsat_constraints, self.slas[iD], self.monitoring_data[iD])
            return 'Constraints not satisfied for SLA ' , iD , ' constraints not satisfied:' , unsat_constraints , ' actions: ' , actions
        else:
            #TODO add id monitoring data
            logging.info('Constraints Satisfied for SLA: ' + iD + ' Monitoring ID: ')
            return 'Constraints Satisfied for SLA ', iD


    def _check_SLA(self, sla, monitoring):
        unsat = []
        solver = z3.Solver()
        solver.add(sla.solver_SLA.assertions())
        solver.add(monitoring.solver_Monitoring.assertions())
        logging.debug('Constraints in the solver for SLA: ' + str(solver))
        control = sla.control_constraints
        check = solver.check(control)
        unsat_list = solver.unsat_core()
        if not unsat_list:
            return 'Satisfied'
        else:
            unsat.extend(str(x).split(':')[0] for x in unsat_list)
            while unsat_list: 
                control = [x for x in control if x not in unsat_list]
                check = solver.check(control)
                unsat_list = solver.unsat_core()
                unsat.extend(str(x).split(':')[0] for x in unsat_list)
            return unsat

    def _start_interfaces(self):
        logging.info('Starting rpc Server')
        s = zerorpc.Server(TCP_Interface(self))
        s.bind("tcp://0.0.0.0:20000")
        self.rpc_interface = gevent.spawn(s.run)

    def receive_ip_instantiated_service(self,service_id, vms_ids, vms_ips):
        print service_id
        print vms_ids
        print vms_ips


if __name__ == '__main__':
    monitor = SLA_Management_Framework()
