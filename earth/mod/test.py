import os
import re
import copy
import math
import yaml
import time
import pickle
import socket
import sqlite3
import fractions
import datetime
import traceback
import subprocess
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import multiprocessing as mp
from multiprocessing import Pool, freeze_support
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
from PyTools.util import utils
from earth.conf import cfg
from earth.conf import glb
from earth.mod import base
from earth.pnl import pnl
from earth.mod import gicsg
from earth.mod import rvg
from earth.alpha import ops


def show_status( d_configs_cfg=cfg.d_configs, d_configs=None ):
    """ show cfg vars and compare. use: run before and after cfg.reload() """
    if d_configs == None:
        d_configs = cfg.d_configs
    print "cfg.d_configs:\n{}\n".format( cfg.d_configs )
    print "d_configs_cfg:\n{}\n".format( d_configs_cfg )
    print "b_decay_cfg:\n{}\n".format( b_decay_cfg )
    print "d_configs:\n{}\n".format( d_configs )
