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
            drone_nm = cursor.execute(status_query, (drone_uuid, )).fetchall()
        try:
            # One of the possible machine status strings
            drone_nm = drone_nm[0][0]
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
        used_vcores = drone_nm_dict['usedVirtualCores']
        free_vcores = drone_nm_dict['availableVirtualCores']
        if used_vcores + free_vcores == 0:
            raise Exception(f'Nodemanager of the drone in the database {drone_nm} has zero free and used cores')
        allocation = used_vcores / (used_vcores + free_vcores)

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
            drone_nm = cursor.execute(status_query, (drone_uuid, )).fetchall()
        try:
            # One of the possible machine status strings
            drone_nm = drone_nm[0][0]
        except:
            raise Exception(
                f'Failed to get nodemanager for drone {drone_uuid} from database')

        with sqlconn:
            cursor = sqlconn.cursor()
            status_query = "SELECT drone_uuid FROM yarn_drones WHERE nm = ? AND status = 'Available'"
            drones = cursor.execute(status_query, (drone_nm, )).fetchall()
        try:
            # Sorted list of all drones
            drones = list(sorted(d[0] for d in drones))
        except:
            raise Exception(
                f'Failed to get drones for nodemanager {drone_nm} from database')

        sqlconn.close()

        rm_nodes = self.rm.nodes
        if not drone_nm in rm_nodes:
            raise Exception(
                f'Nodemanager of the drone in the database {drone_nm} is not in the list of available Yarn nodemanagers {rm_nodes}')

        # Get allocation of this nodemanager
        drone_nm_dict = self.rm.get_node_dict(drone_nm)
        used_vcores = drone_nm_dict['usedVirtualCores']
        free_vcores = drone_nm_dict['availableVirtualCores']
        if used_vcores + free_vcores == 0:
            raise Exception(f'Nodemanager of the drone in the database {drone_nm} has zero free and used cores')

        '''
        # NOTE: We have to subtract here the one always running core to keep the NM alive
        # if we use any vcores. This additional part can either be part of the used vcores
        # or the free vcores.
        effective_used_vcores = used_vcores if used_vcores == 0 else used_vcores - 1
        effective_free_vcores = free_vcores if used_vcores > 0 else free_vcores - 1
        total_cores = effective_used_vcores + effective_free_vcores
        if total_cores == 0:
            utilisation = 1.0 # no core is used but there are also no cores, perfect utilisation!
        else:
            utilisation = effective_used_vcores / total_cores
        '''

        '''
        # This doesn't work because you may remove a core from a NM, which is still running
        # and we cannot distinguish or steer the disintegration more fine-grained.

        # Flag the node as being used if there are at least two used vcores on this NM
        # We ask for at least two because one core is there anyway due to the minimum
        # requirement to keep the NM alive.
        # We have to subtract one core from the used_vcores because this is required to
        # keep the NM running and doesn't count to the condor utilisation.
        utilisation = (used_vcores - 1) / (used_vcores + free_vcores - 1) if used_vcores > 1 else 0.0
        '''

        '''
        # This works but removes just drones on an empty NM

        # Flag the node as being used if there are at least two used vcores on this NM
        # We ask for at least two because one core is there anyway due to the minimum
        # requirement to keep the NM alive.
        utilisation = 1.0 if used_vcores > 1 else 0.0
        '''

        # NOTE: Do we have to account for the always-on vcore of the NM?
        # Is it correct like this?
        used_vcores = used_vcores - 1 if used_vcores > 1 else 0

        if not drone_uuid in drones:
            raise Exception(f'Cannot find drone {drone_uuid} in drones {drones}')

        logger.debug(f'Drones for nodemanager {drone_nm} to get utilisation of drone {drone_uuid}: {drones}')
        logger.debug(f'Used vcores for nodemanager {drone_nm} of drone {drone_uuid}: {used_vcores}')

        idx = drones.index(drone_uuid) + 1 # 1-based indexing on sorted list of drones
        # Keep the first 'used_vcores' number of drones and remove the rest
        utilisation = 1 if idx <= used_vcores else 0
        logger.debug(f'Nodemanager {drone_nm} and drone {drone_uuid} (idx, used vcores, utilisation): {idx}, {used_vcores}, {utilisation}')

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
