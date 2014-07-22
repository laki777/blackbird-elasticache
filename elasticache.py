#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch AWS ElastiCache metrics.
"""

import datetime

from boto.ec2 import cloudwatch

from blackbird.plugins import base


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
                {'LeplicationLag': 'Average'},
                {'LeplicationLag': 'Maximum'},
                {'GetTypeCmds': 'Maximum'},
                {'SetTypeCmds': 'Maximum'},
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
        hostname = self.options.get('hostname')

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
        hostname = self.options.get('hostname')

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

    def _build_ping_item(self):
        return BlackbirdItem(
            key='elasticache.ping',
            value=1,
            host=self.options.get(
                'hostname'
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

        if cache_engine.lower() == 'memcaached':
            pass
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
            "region_name = string(default='us-east-1')",
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
        'region_name': 'ap-northeast-1',
        'aws_access_key_id': 'YOUR_AWS_ACCESS_KEY_ID',
        'aws_secret_access_key': 'YOUR_AWS_SECRET_ACCESS_KEY',
        'cache_engine': 'PLEASE_CHOOSE_CACHE_ENGINE_MEMCACHED_OR_REDIS',
        'cache_cluster_id': 'YOUR_CACHE_CLUSTER_ID',
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

    REDIS_METRICS = JOB._fetch_redis_metrics()
    RESULTS['redis_metric'] = [
        ENTRY.data for ENTRY in REDIS_METRICS
    ]
    RESULTS['blackbird_ping_item'] = JOB._build_ping_item().data
                
    print(json.dumps(RESULTS))
