from ...configuration.configuration import Configuration
from ...interfaces.batchsystemadapter import BatchSystemAdapter
from ...interfaces.batchsystemadapter import MachineStatus
from ...utilities.attributedict import AttributeDict

from typing import Iterable
import logging
import requests
import json
import sqlite3


logger = logging.getLogger('cobald.runtime.tardis.adapters.batchsystem.yarn')


class YarnResourceManager:
    def __init__(self, hostname, ip=8088):
        self.hostname = hostname
        self.ip = ip
        self.base = 'http://{HOSTNAME}:{IP}'.format(HOSTNAME=hostname, IP=ip)

    def _get_nodes(self):
        r = requests.get(self.base + '/ws/v1/cluster/nodes')
        nodes = []
        data = r.json()
        for node in data['nodes']['node']:
            nodes.append(node['id'])
        return nodes

    @property
    def nodes(self):
        return self._get_nodes()

    def get_node_dict(self, node):
        r = requests.get(self.base + '/ws/v1/cluster/nodes/' + node)

        data = r.json()
        return data['node']

    def get_node_resources(self, node, nm_db):
        data = self.get_node_dict(node)

        totalresources = data['totalResource']
        resourceutilization = data['resourceUtilization']

        cpus_query = 'SELECT cpus FROM yarn_nm WHERE name = ?'
        nmconn = sqlite3.connect(nm_db)
        with nmconn:
            nmcursor = nmconn.cursor()
            # Retrieve cpus from current nodemanager entry in the database
            cpus_query_result = nmcursor.execute(
                cpus_query, (node, )).fetchall()

            # Adjust containersCPUUsage value from the YARN REST API. Normalize
            # it to the number of cpus actually in use by YARN on this node
            nm_cpus = cpus_query_result[0][0]
            containers_cpu_usage = resourceutilization['containersCPUUsage'] * \
                nm_cpus / totalresources['vCores']

        nmconn.close()
        return {
            'id': node,
            'cores': totalresources['vCores'],
            'memory': totalresources['memory'],
            'nodeCPUUsage': round(resourceutilization['nodeCPUUsage'], 2),
            'containersCPUUsage': round(containers_cpu_usage, 2), }

    def set_allocated_resources(self, node, cores, memory):
        payload = {'resource': {'memory': memory,
                                'vCores': cores}, 'overCommitTimeout': -1}
        requests.post(self.base + '/ws/v1/cluster/nodes/'
                      + node + '/resource', json=payload)


class YarnAdapter(BatchSystemAdapter):
    """
    :py:class:`~tardis.adapters.batchsystems.yarn.YarnAdapter` implements
    the TARDIS interface to dynamically integrate and manage opportunistic resources
    with the Yarn system.
    """

    def __init__(self):
        logger.debug('Initialize Yarn BatchSystemAdapter')
        self.config = Configuration()
        logger.debug(
            f'Register Yarn resourcemanager on hostname {self.config.BatchSystem.resourcemanager}')
        logger.debug(
            f'Register Yarn drones database at {self.config.BatchSystem.drones_database}')
        self.rm = YarnResourceManager(self.config.BatchSystem.resourcemanager)

    async def disintegrate_machine(self, drone_uuid: str) -> None:
        """
        Yarn does not require any specific disintegration procedure.

        :param drone_uuid: Uuid of the worker node, for some sites corresponding
            to the host name of the drone.
        :type drone_uuid: str
        :return: None
        """
        return

    async def drain_machine(self, drone_uuid: str) -> None:
        """
        Drain a machine in the HTCondor batch system, which means that no new
        jobs will be accepted

        :param drone_uuid: Uuid of the worker node, for some sites corresponding
            to the host name of the drone.
        :type drone_uuid: str
        :return: None
        """
        sqlconn = sqlite3.connect(self.config.BatchSystem.drones_database)
        cursor = sqlconn.cursor()

        drain_query = """UPDATE yarn_drones
                       SET status = 'Draining'
                       WHERE drone_uuid = ?"""
        cursor.execute(drain_query, (drone_uuid, ))
        sqlconn.commit()
        sqlconn.close()
        logger.debug(f'Drain machine {drone_uuid}')

    async def integrate_machine(self, drone_uuid: str) -> None:
        """
        Yarn does not require any specific integration procedure

        :param drone_uuid: Uuid of the worker node, for some sites corresponding
            to the host name of the drone.
        :type drone_uuid: str
        :return: None
        """
        return None

    async def get_allocation(self, drone_uuid: str) -> float:
        """
        Get the allocation of the YARN NodeManager, which is defined as maximum
        of the ratios of requested over total resources (CPU or Memory).
        """
        # Find nodemanager responsible for this drone
        sqlconn = sqlite3.connect(self.config.BatchSystem.drones_database)
        with sqlconn:
            cursor = sqlconn.cursor()
            status_query = "SELECT nm FROM yarn_drones WHERE drone_uuid = ?"
            results = cursor.execute(status_query, (drone_uuid, )).fetchall()
        try:
            # One of the possible machine status strings
            drone_nm = results[0][0]
        except:
            raise Exception(
                f'Failed to get nodemanager for drone {drone_uuid} from database')
        sqlconn.close()

        rm_nodes = self.rm.nodes
        if not drone_nm in rm_nodes:
            raise Exception(
                f'Nodemanager of the drone in the database {drone_nm} is not in the list of available Yarn nodemanagers {rm_nodes}')

        # Get allocation of this nodemanager
        drone_nm_dict = self.rm.get_node_dict(drone_nm)
        total_vcores = drone_nm_dict['totalResource']['vCores']
        used_vcores = drone_nm_dict['usedVirtualCores']
        allocation = used_vcores / total_vcores

        logger.debug(
            f'Get allocation for nodemanager {drone_nm} of drone {drone_uuid}: {allocation}')
        return allocation

    async def get_machine_status(self, drone_uuid: str) -> MachineStatus:
        """
        Get the status of a worker node in Yarn (Available, Draining,
        Drained, NotAvailable)

        :param drone_uuid: Uuid of the worker node, for some sites corresponding
            to the host name of the drone.
        :type drone_uuid: str
        :return: The machine status in Yarn (Available, Draining, Drained,
            NotAvailable)
        :rtype: MachineStatus
        """
        logger.debug(f'Get status for machine {drone_uuid}')
        sqlconn = sqlite3.connect(self.config.BatchSystem.drones_database)
        with sqlconn:
            cursor = sqlconn.cursor()
            status_query = "SELECT status FROM yarn_drones WHERE drone_uuid = ?"
            results = cursor.execute(status_query, (drone_uuid, )).fetchall()
        # results is a list of tuples, we're interested in the first element of the first tuple
        try:
            # One of the possible machine status strings
            drone_status = results[0][0]
        except:
            logger.debug(
                f'Failed to fetch status of drone {drone_uuid}, set to Draining')
            drone_status = 'Draining'
        logger.debug(f'Status is {drone_status}')

        sqlconn.close()
        return getattr(MachineStatus, drone_status)

    async def get_utilisation(self, drone_uuid: str) -> float:
        """
        Get the utilisation of a worker node in Yarn, which is defined as
        minimum of the ratios of requested over total resources
        (CPU, Memory, Disk, etc.).

        :param drone_uuid: Uuid of the worker node, for some sites corresponding
            to the host name of the drone.
        :type drone_uuid: str
        :return: The utilisation of a worker node as described above.
        :rtype: float
        """
        # Find nodemanager responsible for this drone
        sqlconn = sqlite3.connect(self.config.BatchSystem.drones_database)
        with sqlconn:
            cursor = sqlconn.cursor()
            status_query = "SELECT nm FROM yarn_drones WHERE drone_uuid = ?"
            results = cursor.execute(status_query, (drone_uuid, )).fetchall()
        try:
            # One of the possible machine status strings
            drone_nm = results[0][0]
        except:
            raise Exception(
                f'Failed to get nodemanager for drone {drone_uuid} from database')
        sqlconn.close()

        rm_nodes = self.rm.nodes
        if not drone_nm in rm_nodes:
            raise Exception(
                f'Nodemanager of the drone in the database {drone_nm} is not in the list of available Yarn nodemanagers {rm_nodes}')

        # Get cpu usage of this nodemanager
        nodemanager_db = self.config.BatchSystem.nodemanager_database
        nm_metrics = self.rm.get_node_resources(drone_nm, nodemanager_db)
        utilisation = float(nm_metrics['containersCPUUsage']) / float(nm_metrics['cores'])

        logger.debug(
            f'Get utilisation for nodemanager {drone_nm} of drone {drone_uuid}: {utilisation}')
        return utilisation

    @property
    def machine_meta_data_translation_mapping(self) -> AttributeDict:
        """
        The machine meta data translation mapping is used to translate units of
        the machine meta data in ``TARDIS`` to values expected by the
        Yarn batch system adapter.

        :return: Machine meta data translation mapping
        :rtype: AttributeDict
        """
        return AttributeDict(Cores=1, Memory=1, Disk=1)
