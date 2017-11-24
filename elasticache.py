#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch AWS ElastiCache metrics.
http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/CacheMetrics.HostLevel.html
http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/CacheMetrics.Redis.html
http://docs.aws.amazon.com/AmazonElastiCache/latest/UserGuide/CacheMetrics.Memcached.html
https://github.com/Vagrants/blackbird-elasticache
"""
import datetime, sys
from boto.ec2 import cloudwatch
from blackbird.plugins import base
from pyzabbix import ZabbixMetric, ZabbixSender

class ConcreteJob(base.JobBase):

 def __init__(self, options, queue=None, logger=None):
  super(ConcreteJob, self).__init__(options, queue, logger)
  self.metrics_config = {
   'host_level': [
    {'CPUUtilization': 'Average'},
    {'SwapUsage': 'Average'},
    {'FreeableMemory': 'Average'},
    {'NetworkBytesIn': 'Average'},
    {'NetworkBytesOut': 'Average'},
   ],
   'redis': [
    {'CurrConnections': 'Average'},
    {'CurrConnections': 'Maximum'},
    {'Evictions': 'Average'},
    {'Evictions': 'Maximum'},
    {'Reclaimed': 'Average'},
    {'Reclaimed': 'Maximum'},
    {'NewConnections': 'Average'},
    {'NewConnections': 'Maximum'},
    {'BytesUsedForCache': 'Maximum'},
    {'CacheHits': 'Average'},
    {'CacheHits': 'Maximum'},
    {'CacheMisses': 'Average'},
    {'CacheMisses': 'Maximum'},
    {'ReplicationLag': 'Average'},
    {'ReplicationLag': 'Maximum'},
    {'GetTypeCmds': 'Maximum'},
    {'SetTypeCmds': 'Maximum'},
    {'KeyBasedCmds': 'Maximum'},
    {'StringBasedCmds': 'Maximum'},
    {'HashBasedCmds': 'Maximum'},
    {'ListBasedCmds': 'Maximum'},
    {'SetBasedCmds': 'Maximum'},
    {'SortedSetBasedCmds': 'Maximum'},
    {'CurrItems': 'Maximum'},
   ],
   'memcached': [
    {'CurrConnections': 'Average'},
    {'CurrConnections': 'Maximum'},
    {'Evictions': 'Average'},
    {'Evictions': 'Maximum'},
    {'Reclaimed': 'Average'},
    {'Reclaimed': 'Maximum'},
    {'NewConnections': 'Average'},
    {'NewConnections': 'Maximum'},
    {'BytesUsedForCacheItems': 'Maximum'},
    {'CasHits': 'Average'},
    {'CasHits': 'Maximum'},
    {'CasMisses': 'Average'},
    {'CasMisses': 'Maximum'},
    {'CmdGet': 'Maximum'},
    {'CmdSet': 'Maximum'},
    {'KeyBasedCmds': 'Maximum'},
    {'StringBasedCmds': 'Maximum'},
    {'HashBasedCmds': 'Maximum'},
    {'ListBasedCmds': 'Maximum'},
    {'SetBasedCmds': 'Maximum'},
    {'SortedSetBasedCmds': 'Maximum'},
    {'CurrItems': 'Maximum'},
   ]
  }

 def _create_connection(self):
  conn = cloudwatch.connect_to_region(
   self.options.get('region_name'),
   aws_access_key_id=self.options.get(
    'aws_access_key_id'
   ),
   aws_secret_access_key=self.options.get(
    'aws_secret_access_key'
   )
  )
  return conn

 def _fetch_host_level_metrics(self):
  conn = self._create_connection()
  result = list()

  ignore_metrics = self.options.get('ignore_metrics', list())
  period = int(self.options.get('interval', 60))
  if period <= 60:
   period = 60
   delta_seconds = 120
  else:
   delta_seconds = period
  end_time = datetime.datetime.utcnow()
  start_time = end_time - datetime.timedelta(
   seconds=delta_seconds
  )
  dimensions = {
   'CacheClusterId': self.options.get(
    'cache_cluster_id'
   ),
   'CacheNodeId': self.options.get(
    'cache_node_id'
   )
  }
  hostname = self.options.get('hostname', 'zabbix_server_name')

  for entry in self.metrics_config['host_level']:
   for metric_name, statistics in entry.iteritems():
    if not metric_name in ignore_metrics:
     metric = conn.get_metric_statistics(
      period=period,
      start_time=start_time,
      end_time=end_time,
      metric_name=metric_name,
      namespace='AWS/ElastiCache',
      statistics=statistics,
      dimensions=dimensions
     )
     #print metric
     if len(metric) > 0:
      result.append(
       ElastiCacheHostItem(
        key=metric_name,
        value=str(metric[0][statistics]),
        host=hostname
       )
      )

  conn.close()
  return result

 def _fetch_redis_metrics(self):
  conn = self._create_connection()
  result = list()

  ignore_metrics = self.options.get('ignore_metrics', list())
  period = int(self.options.get('interval', 60))
  if period <= 60:
   period = 60
   delta_seconds = 120
  else:
   delta_seconds = period
  end_time = datetime.datetime.utcnow()
  start_time = end_time - datetime.timedelta(
   seconds=delta_seconds
  )
  dimensions = {
   'CacheClusterId': self.options.get(
    'cache_cluster_id'
   ),
   'CacheNodeId': self.options.get(
    'cache_node_id'
   )
  }
  hostname = self.options.get('hostname', 'zabbix_server_name')
  for entry in self.metrics_config['redis']:
   for metric_name, statistics in entry.iteritems():
    if not metric_name in ignore_metrics:
     metric = conn.get_metric_statistics(
      period=period,
      start_time=start_time,
      end_time=end_time,
      metric_name=metric_name,
      namespace='AWS/ElastiCache',
      statistics=statistics,
      dimensions=dimensions
     )
     if len(metric) > 0:
      key = '{0}.{1}'.format(
       metric_name,
       statistics
      )
      result.append(
       ElastiCacheRedisItem(
        key=key,
        value=str(metric[0][statistics]),
        host=hostname
       )
      )

  conn.close()
  return result

 def _fetch_memcached_metrics(self):
  conn = self._create_connection()
  result = list()

  ignore_metrics = self.options.get('ignore_metrics', list())
  period = int(self.options.get('interval', 60))
  if period <= 60:
   period = 60
   delta_seconds = 120
  else:
   delta_seconds = period
  end_time = datetime.datetime.utcnow()
  start_time = end_time - datetime.timedelta(
   seconds=delta_seconds
  )
  dimensions = {
   'CacheClusterId': self.options.get(
    'cache_cluster_id'
   ),
   'CacheNodeId': self.options.get(
    'cache_node_id'
   )
  }
  hostname = self.options.get('hostname', 'zabbix_server_name')

  for entry in self.metrics_config['memcached']:
   for metric_name, statistics in entry.iteritems():
    if not metric_name in ignore_metrics:
     metric = conn.get_metric_statistics(
      period=period,
      start_time=start_time,
      end_time=end_time,
      metric_name=metric_name,
      namespace='AWS/ElastiCache',
      statistics=statistics,
      dimensions=dimensions
     )
     if len(metric) > 0:
      key = '{0}.{1}'.format(
       metric_name,
       statistics
      )
      result.append(
       ElastiCacheMemcachedItem(
        key=key,
        value=str(metric[0][statistics]),
        host=hostname
       )
      )

  conn.close()
  return result

 def _build_ping_item(self):
  return BlackbirdItem(
   key='elasticache.ping',
   value=1,
   host=self.options.get(
    'hostname', 'zabbix.arbor.sc'
   )
  )

 def build_items(self):
  """
  Main loop
  """
  items = list()
  items.extend(self._fetch_host_level_metrics())
  items.append(self._build_ping_item())

  cache_engine = self.options.get('cache_engine')

  if cache_engine.lower() == 'memcached':
   items.extend(self._fetch_memcached_metrics())
  elif cache_engine.lower() == 'redis':
   items.extend(self._fetch_redis_metrics())
  else:
   raise base.BlackbirdPluginError(
    'Supported Cache Engines are only "memcached", "redis"!!'
   )

  for entry in items:
   if self.enqueue(entry):
    self.logger.debug(
     'Enqueued item. {0}'.format(entry.data)
    )


class Validator(base.ValidatorBase):
 """
 Validate configuration object.
 """
 def __init__(self):
  self.__spec = None

 @property
 def spec(self):
  self.__spec = (
   "[{0}]".format(__name__),
   "region_name = string(default='us-east-2')",
   "aws_access_key_id = string()",
   "aws_secret_access_key = string()",
   "cache_engine = string()",
   "cache_cluster_id = string()",
   "cache_node_id = string(default='0001')",
   "hostname = string()",
   "ignore_metrics = list(default=list())"
  )
  return self.__spec

class ElastiCacheHostItem(base.ItemBase):
 """
 Enqueued item.
 """

 def __init__(self, key, value, host):
  super(ElastiCacheHostItem, self).__init__(key, value, host)

  self.__data = dict()
  self._generate()

 @property
 def data(self):
  """
  Dequeued data.
  """
  return self.__data

 def _generate(self):
  self.__data['key'] = 'cloudwatch.elasticache.host.{0}'.format(self.key)
  self.__data['value'] = self.value
  self.__data['host'] = self.host
  self.__data['clock'] = self.clock


class ElastiCacheRedisItem(base.ItemBase):
 """
 Enqueued item.
 """

 def __init__(self, key, value, host):
  super(ElastiCacheRedisItem, self).__init__(key, value, host)

  self.__data = dict()
  self._generate()

 @property
 def data(self):
  """
  Dequeued data.
  """
  return self.__data

 def _generate(self):
  self.__data['key'] = 'cloudwatch.elasticache.redis.{0}'.format(self.key)
  self.__data['value'] = self.value
  self.__data['host'] = self.host
  self.__data['clock'] = self.clock

class ElastiCacheMemcachedItem(base.ItemBase):
 """
 Enqueued item.
 """

 def __init__(self, key, value, host):
  super(ElastiCacheMemcachedItem, self).__init__(key, value, host)

  self.__data = dict()
  self._generate()

 @property
 def data(self):
  """
  Dequeued data.
  """
  return self.__data

 def _generate(self):
  self.__data['key'] = 'cloudwatch.elasticache.memcached.{0}'.format(self.key)
  self.__data['value'] = self.value
  self.__data['host'] = self.host
  self.__data['clock'] = self.clock

class BlackbirdItem(base.ItemBase):
 """
 Enqueued item.
 """

 def __init__(self, key, value, host):
  super(BlackbirdItem, self).__init__(key, value, host)

  self.__data = dict()
  self._generate()

 @property
 def data(self):
  """
  Dequeued data.
  """
  return self.__data

 def _generate(self):
  self.__data['key'] = 'blackbird.{0}'.format(self.key)
  self.__data['value'] = self.value
  self.__data['host'] = self.host
  self.__data['clock'] = self.clock


if __name__ == '__main__':
 import json
 OPTIONS = {
  'region_name': 'eu-west-2',
  'aws_access_key_id': '****************',
  'aws_secret_access_key': '****************',
  'cache_engine': str(sys.argv[1]),
  'cache_cluster_id': str(sys.argv[2]),
  'cache_node_id': '0001',
  'interval': 60,
  'ignore_metrics': list(),
 }
 RESULTS = dict()
 JOB = ConcreteJob(options=OPTIONS)

 HOST_LEVEL_METRICS = JOB._fetch_host_level_metrics()
 RESULTS['host_level_metrics'] = [
  ENTRY.data for ENTRY in HOST_LEVEL_METRICS
 ]

 if str(sys.argv[1]) == 'redis':
  REDIS_METRICS = JOB._fetch_redis_metrics()
  RESULTS['redis_metric'] = [
   ENTRY.data for ENTRY in REDIS_METRICS
  ]
 else:
  MEMCACHED_METRICS = JOB._fetch_memcached_metrics()
  RESULTS['memcached_metrics'] = [
   ENTRY.data for ENTRY in MEMCACHED_METRICS
  ]

 RESULTS['blackbird_ping_item'] = JOB._build_ping_item().data

 data = json.dumps(RESULTS)
 #print(json.dumps(RESULTS))

 data1 = json.loads(data)

 if str(sys.argv[1]) == 'memcached':
  items = ['blackbird_ping_item', 'host_level_metrics', 'memcached_metrics']
 else:
  items = ['blackbird_ping_item', 'host_level_metrics', 'redis_metric']

for i in range(len(items)):
 if items[i] == 'blackbird_ping_item':
  packet = [ ZabbixMetric(str(sys.argv[2]), data1[items[i]]['key'], data1[items[i]]['value']) ]
  ZabbixSender(use_config=True).send(packet)
 else:
  for k in range(len(data1[items[i]])):
   packet = [ ZabbixMetric(str(sys.argv[2]), data1[items[i]][k]['key'], data1[items[i]][k]['value']) ]
   ZabbixSender(use_config=True).send(packet)
