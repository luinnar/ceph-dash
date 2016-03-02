#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import json
import subprocess
import os

from flask import request
from flask import render_template
from flask import abort
from flask import jsonify
from flask import current_app
from flask.views import MethodView
from app.base import ApiResource


class CephClusterCommand(dict):
    """
    Issue a ceph command on the given cluster and provide the returned json
    """

    def __init__(self, command, options):
        dict.__init__(self)

        # hardcoded commands
        if command not in ['status', 'osd tree']:
            raise ValueError('Command {0} is not allowed'.format(command))

        user    = options['mon_user']
        hosts   = options['mon_host']
        error   = None
        result  = None

        if not isinstance(hosts, list):
            hosts = [hosts]

        for host in hosts:
            # tries connect to mons until some returns correct response
            shellCmd = 'ssh {0}@{1} "ceph {2} --format=json"'.format(user, host, command)

            try:
                result  = subprocess.check_output(shellCmd, shell=True, universal_newlines=True)
                result  = json.loads(result.strip())
                error   = None
                break   # everything went good

            except Exception as e:
                result  = None
                error   = e

        if result is not None:
            self.update(result)
        elif error is not None:
           self['err'] = str(error)


def find_host_for_osd(osd, osd_status):
    """ find host for a given osd """

    for obj in osd_status['nodes']:
        if obj['type'] == 'host':
            if osd in obj['children']:
                return obj['name']

    return 'unknown'


def get_unhealthy_osd_details(osd_status):
    """ get all unhealthy osds from osd status """

    unhealthy_osds = list()

    for obj in osd_status['nodes']:
        if obj['type'] == 'osd':
            # if OSD does not exists (DNE in osd tree) skip this entry
            if obj['exists'] == 0:
                continue
            if obj['status'] == 'down' or obj['status'] == 'out':
                # It is possible to have one host in more than one branch in the tree.
                # Add each unhealthy OSD only once in the list
                entry = {
                    'name': obj['name'],
                    'status': obj['status'],
                    'host': find_host_for_osd(obj['id'], osd_status)
                }
                if entry not in unhealthy_osds:
                    unhealthy_osds.append(entry)

    return unhealthy_osds


class DashboardResource(ApiResource):
    """
    Endpoint that shows overall cluster status
    """

    endpoint = 'dashboard'
    url_prefix = '/'
    url_rules = {
        'index': {
            'rule': '/',
        }
    }

    def __init__(self):
        MethodView.__init__(self)
        self.config = current_app.config['USER_CONFIG']

    def get(self):

        cluster_status = CephClusterCommand('status', options=self.config)
        if 'err' in cluster_status:
            abort(500, cluster_status['err'])

        # check for unhealthy osds and get additional osd infos from cluster
        total_osds = cluster_status['osdmap']['osdmap']['num_osds']
        in_osds = cluster_status['osdmap']['osdmap']['num_up_osds']
        up_osds = cluster_status['osdmap']['osdmap']['num_in_osds']

        if up_osds < total_osds or in_osds < total_osds:
            osd_status = CephClusterCommand('osd tree', options=self.config)
            if 'err' in osd_status:
                abort(500, osd_status['err'])

            # find unhealthy osds in osd tree
            cluster_status['osdmap']['details'] = get_unhealthy_osd_details(osd_status)

        if request.mimetype == 'application/json':
            return jsonify(cluster_status)
        else:
            return render_template('status.html', data=cluster_status, config=self.config)
