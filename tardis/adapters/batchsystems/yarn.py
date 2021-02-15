from ...configuration.configuration import Configuration
from ...interfaces.batchsystemadapter import BatchSystemAdapter
from ...interfaces.batchsystemadapter import MachineStatus
from ...utilities.attributedict import AttributeDict

from typing import Iterable
import logging
import requests
import json
import sqlite3


logger = logging.getLogger("cobald.runtime.tardis.adapters.batchsystem.yarn")


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

    def get_allocated_resources(self, node):
        r = requests.get(self.base + '/ws/v1/cluster/nodes/' + node)
        data = r.json()
        resources = data['node']['totalResource']
        return {'cores': resources['vCores'], 'memory': resources['memory']}

    def get_used_vcores(self, node):
        r = requests.get(self.base + '/ws/v1/cluster/nodes/' + node)
        data = r.json()
        return data['node']['usedVirtualCores']

    def set_allocated_resources(self, node, cores, memory):
        payload = {"resource": {"memory": memory, "vCores": cores}, "overCommitTimeout": -1}
        r = requests.post(self.base + '/ws/v1/cluster/nodes/' + node + '/resource', json=payload)


class YarnAdapter(BatchSystemAdapter):
    """
    :py:class:`~tardis.adapters.batchsystems.yarn.YarnAdapter` implements
    the TARDIS interface to dynamically integrate and manage opportunistic resources
    with the Yarn system.
    """

    def __init__(self):
        logger.debug('Initialize Yarn BatchSystemAdapter')
        self.config = Configuration()
        logger.debug(f'Register Yarn resourcemanager on hostname {self.config.BatchSystem.resourcemanager}')
        logger.debug(f'Register Yarn drones database at {self.config.BatchSystem.drones_database}')
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
        allocated_resources = self.rm.get_allocated_resources(self.rm.nodes[0])
        allocated_vcores = allocated_resources['cores']
        used_vcores = self.rm.get_used_vcores(self.rm.nodes[0])
        utilisation = used_vcores / allocated_vcores
        logger.debug(f'Get allocation for machine {drone_uuid} (equal to utilisation): {utilisation}')
        return utilisation

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
            drone_status = results[0][0] # One of the possible machine status strings
        except:
            logger.debug(f'Failed to fetch status of drone {drone_uuid}, set to Draining')
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
        allocated_resources = self.rm.get_allocated_resources(self.rm.nodes[0])
        allocated_vcores = allocated_resources['cores']
        used_vcores = self.rm.get_used_vcores(self.rm.nodes[0])
        utilisation = used_vcores / allocated_vcores
        logger.debug(f'Get utilisation for machine {drone_uuid}: {utilisation}')
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
