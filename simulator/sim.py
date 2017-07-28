#!/usr/bin/python

""" Master coordinator """

from __future__ import print_function
import os
import sys
import subprocess
import string
import logging
import logging.config
import argparse
import datetime
import operator
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
work_dir = "$HOME/prj"
if os.getcwd() != "{}/simulator".format( work_dir ):
    sys.path = [ path for path in sys.path if "simulator" not in path and "earth" not in path ]
    sys.path += [ os.getcwd(), "{}/../earth".format( os.getcwd() ) ]
l_lib = [ path for path in sys.path if "simulator" in path or "earth" in path ]
print ( "Library paths: {}\n".format( l_lib ) )

import matplotlib
matplotlib.use('Agg')
from sol.core import basic as solbasic
from sol.core import date as soldate
from earth.conf import cfg
from earth.conf import glb
from earth.cache import dmgr
from earth.cache import preparer
from earth.cache import cutils
from earth.mod import base
from earth.pnl import pnl
from earth.mod import base_etf
from earth.pnl import pnl_etf
from earth.mod import univ
from earth.mod import adj
from earth.mod import cs
from earth.mod import csf
from earth.mod import csn
from earth.mod import csq
from earth.mod import csa
from earth.mod import gicsg
from earth.mod import rvg
from earth.mod import test
from earth.alpha import alp
from earth.alpha import ops
from earth.alpha import autils


if __name__ == "__main__":

    parser = argparse.ArgumentParser( description='Master coordinator.' )
    parser.add_argument( '-l', '--l', '-log', '--log', metavar="log_level", type=str, default="DEBUG", 
                   help='Log level: debug, info, warning, error, critical. Default debug.' )
    parser.add_argument('-dryrun', '--dryrun', action='store_true', help="Dry run, no cache or alpha write.")
    parser.add_argument('-rebuild', '--rebuild', action='store_true', help="Rebuild cache.")
    parser.add_argument('-refresh', '--refresh', action='store_true', help="Refresh cache with new data.")
    parser.add_argument( '-m', '--m', "-sim_mode", "--sim_mode", metavar="sim_mode", type=str, default=None, 
                   help="Simulation mode. Default manual." )
    parser.add_argument( '-mn', '--mn', "-mode_neutral", "--mode_neutral", metavar="mode_neutral", type=int, default=None, 
                   help="Neutral mode. Default manual." )
    parser.add_argument( '-d', '--d', "-delay", "--delay", metavar="delay", type=int, default=None, 
                   help="Neutral mode. Default manual." )
    parser.add_argument( '-u', '--u', "-univ", "--univ", metavar="n_in_play", type=int, default=None, 
                   help="Universe size. Default manual." )
    parser.add_argument( '-p', '--p', "-sim_period", "--sim_period", metavar="sim_period", type=str, default=None, 
                   help="Simulation period. Default custom." )
    parser.add_argument( '-np', '--np', "-n_processes", "--n_processes", metavar="n_processes", type=int, default=None, 
                   help="Number of simulation processes. Default cfg.n_processes." )
    parser.add_argument( '-data', '--data', metavar="dataset", type=str, default=None, 
                   help="Simulation dataset. Currently support cs, smt, de. Default manual." )
    parser.add_argument( '-id', '--id', metavar="identity", type=str, default=None, 
                   help="Identity of current run." )
    parser.add_argument( '-combo_prod', '--combo_prod', action='store_true', help="Run specified combos in config only." )
    args = parser.parse_args()

    # set logging level
    log_level = args.l 
    solbasic.logger = solbasic.set_log_level( log_level )
    if log_level.lower() != "debug":
        solbasic.logger.info( "Set log level as {}.".format( log_level ) )

    if args.p:
        cfg.sim_period = args.p
    if args.d:
        cfg.delay = args.d
    if args.u:
        cfg.n_in_play = args.u
    if args.data:
        cfg.dataset = args.data
    if args.mn != None:
        cfg.mode_neutral = args.mn
    if args.rebuild:
        cfg.b_recreate_cache = True
        cfg.b_dump_pos_only = True
    if args.refresh:
        cfg.b_refresh_cache = True
        cfg.b_dump_pos_only = True
    b_combo_prod = args.combo_prod
    if args.dryrun:
        cfg.b_dryrun = True
    if args.m:
        cfg.sim_mode = args.m
    if args.np:
        cfg.n_processes = args.np
    if cfg.sim_mode in [ "alphacache", "combo" ] and args.rebuild:
        cfg.b_recreate_cache = False
        cfg.b_recreate_alpha_cache = True
    cfg.reload( cfg )

    print( "start_date = {}, end_date = {}".format( cfg.start_date, cfg.end_date ) )

    if -cfg.sim_days_start < -cfg.load_days_start and cfg.load_days_start > 0:
        print( "Warning: sim_days_start ({0}) < load_days_start ({1}).".format(-cfg.sim_days_start, -cfg.load_days_start) )

    print( "\nGlobal settings:" )
    print( "------------------------------" )
    print( "sim_period={}, sim_mode={}, dataset={}, iden_stock={}, n_in_play={}, delay={}, cache range {}-{}, load_backdays={}, simulation range [{}, {}, {}), mode_pnl={}, mode_tcost={}, mode_neutral={}, n_min_group={}".format( cfg.sim_period, cfg.sim_mode, cfg.dataset, cfg.iden_stock, cfg.n_in_play, cfg.delay, cfg.start_date, cfg.end_date, cfg.load_backdays, -cfg.sim_days_start, -cfg.sim_days_is_vs, -cfg.sim_days_end, cfg.mode_pnl, cfg.mode_tcost, cfg.mode_neutral, cfg.n_min_group ) )
    print( "b_alphagen_on={}, b_dryrun={}, b_prepare_data={}, b_recreate_cache={}, b_refresh_cache={}, b_load_cache_from_db={}, b_dump_pos_only={}, b_init_filter={}, b_conduct_tests={}, b_conduct_corr_screen={}, b_recompute_alpha={}, n_id_skip_till={}, b_keep_gen={}, b_keep_id={}, b_process_alpha_dump={}, mem_thold={}, d_mem_proc={}, b_combo={}, b_plot={}".format( cfg.b_alphagen_on, cfg.b_dryrun, cfg.b_prepare_data, cfg.b_recreate_cache, cfg.b_refresh_cache, cfg.b_load_cache_from_db, cfg.b_dump_pos_only, cfg.b_init_filter, cfg.b_conduct_tests, cfg.b_conduct_corr_screen, cfg.b_recompute_alpha, cfg.n_id_skip_till, cfg.b_keep_gen, cfg.b_keep_id, cfg.b_process_alpha_dump, cfg.mem_thold, cfg.d_mem_proc, cfg.b_combo, cfg.b_plot ) )
    print( "name_batch={}\ntarget_dir={}\nstr_db_position={}".format( cfg.name_batch, cfg.target_dir, cfg.str_db_position ) )
    if cfg.sim_period in [ "forward", "live" ]:
        print( "cache path: {}*{}_forward.h5".format( cfg.path_cache, cfg.start_date ) )
    else:
        print( "cache path: {}*{}_{}.h5".format( cfg.path_cache, cfg.start_date, cfg.end_date ) )
    l_alpha_config = []
    for key, value in sorted( cfg.d_configs.iteritems(), key=operator.itemgetter(0) ):
        l_alpha_config.append( "{}=\"{}\"".format( key, value ) )
    print( "alpha configs: {}".format( ", ".join( l_alpha_config ) ) )
    print( "------------------------------\n" )

    pd.set_option('display.max_rows', cfg.max_rows)
    pd.set_option('display.max_columns', cfg.max_columns)

    # prepare data
    prep = preparer.preparer()
    glb.prep = prep
    if cfg.b_alpha_combo:
        prep.base_data_combo()
    else:
        if cfg.b_prepare_data:
            prep.base_data( load_type="base" )
            if "cs" in cfg.dataset:
                csit, csfit, csnit, csqit, csait = prep.compustat()
                if isinstance( csit, HDFStore ):
                    cs = csit
                    csf = csfit
                    csn = csnit
                    csq = csqit
                    csa = csait
            if "ciq" in cfg.dataset:
                prep.capital_iq()
            if "ws" in cfg.dataset:
                prep.worldscope()
            if "rtf" in cfg.dataset:
                prep.reuters_fundamental()
            if "fsf" in cfg.dataset:
                prep.factset_fundamental( d_fields_used )
            if "etf" in cfg.dataset:
                prep.base_data_etf()
        else:
            prep.base_data( load_type="pnl" )
        if cfg.mode_neutral > 0 and cfg.mode_neutral <= 3:
            prep.gics_groups()
        #if cfg.mode_neutral > 3:
        #    prep.rv_groups()
    if cfg.sim_mode == "cache":
        solbasic.logger.info( "Requested caches loaded." )
    else:
        solbasic.logger.info( "All data loaded." )

    cmd = "renice -n {} -g {}".format( cfg.niceness, os.getpid() ).split()
    sp = subprocess.call( cmd )

    if cfg.b_alphagen_on or cfg.b_process_alpha_dump:
        if cfg.b_load_cache_from_db:
            l_aid = []
            pass
        else:
            l_aid = cutils.load_stats_aid( cfg.path_alpha_pool )
            # preselection specified here for local batch
        l_aid_wanted = sorted( l_aid )
        if cfg.sim_mode == "alpharef":
            solbasic.logger.info( "Final list of {} alphas selected for refresh:\n{}".format( len( l_aid_wanted ), l_aid_wanted ) )

    # alpha generation
    if cfg.b_alphagen_on:
        print( "\nGeneration settings:" )
        print( "------------------------------" )
        print( "n_pop={}, n_gen={}, n_processes={}, n_min_height={}, n_max_height={}, n_id_start={}".format( cfg.n_pop, cfg.n_gen, cfg.n_processes, cfg.n_min_height, cfg.n_max_height, cfg.n_id_start ) )
        print( "------------------------------\n" )
        solbasic.logger.info( "Alpha generation begins." )
        solbasic.logger.info( "Registering primitives and terminals..." )
        pass
        
    # turn on pop loading feature
    else:
        pass

    str_today = datetime.datetime.strftime( datetime.datetime.now(), "%Y%m%d" )
    fname_fig = cfg.path_alpha_pool.replace(".pkl", "_{}.png".format( str_today ) )

 
    # plot all alphas in list, and all combos if applicable
    if cfg.b_plot and not cfg.b_dryrun:
        fname_fig_tcost100 = fname_fig.replace( str_today, "tcost100_" + str_today )
        fname_fig_combo = "{}{}_{}_combo_-{}_-{}_{}.png".format( cfg.path_repo, cfg.name_batch, cfg.mode_neutral, sim_days_start, sim_days_end, str_today )
        if b_combo_prod:
            fname_fig_combo = "{}{}_{}_combo_prod_-{}_-{}_{}.png".format( cfg.path_repo, cfg.name_batch, cfg.mode_neutral, sim_days_start, sim_days_end, str_today )
        fname_fig_combo_tcost100 = fname_fig_combo.replace( str_today, "tcost100_" + str_today )

        # plot combo alpha pnl
        if cfg.sim_mode == "comboplot":
            l_alph_combo = cutils.load_stats_cache_dirs_combo( cfg.path_combo )
            if b_combo_prod:
                l_alph_combo = [ alph for alph in l_alph_combo if alph.name in cfg.l_cname ]
                l_alph_names = [ alph.name for alph in l_alph_combo ]
                solbasic.logger.info( "Plotting {} from {} alpha stats caches...".format( l_alph_names, len( l_alph_combo ) ) )

        if cfg.b_combo or cfg.sim_mode == "comboplot":
            solbasic.logger.info( "Plotting tier 0 alpha performance..." )
            autils.plot_pnl( l_alph_combo, fig_name=fname_fig_combo )
            solbasic.logger.info( "Plotting tier 100 alpha performance..." )
            autils.plot_pnl( l_alph_combo, fig_name=fname_fig_combo_tcost100, tier=100 )
            solbasic.logger.info( "Plotted combos in [{},{}) as:\n{}\n{}.\n".format( -sim_days_start, -sim_days_end, fname_fig_combo, fname_fig_combo_tcost100 ) )
            cmd = "cp -rp {} {}/pnl/{}/".format( fname_fig_combo, work_dir, cfg.sim_period ).split()
            sp = subprocess.call( cmd )
            cmd = "cp -rp {} {}/pnl/{}/".format( fname_fig_combo_tcost100, work_dir, cfg.sim_period ).split()
            sp = subprocess.call( cmd )
            plt.close( "all" )
        # plot pool alpha pnl
        else:
            solbasic.logger.info( "Plotting tier 0 alpha performance..." )
            autils.plot_pnl( l_alph, fig_name=fname_fig )
#            solbasic.logger.info( "Plotting tier 100 alpha performance..." )
#            autils.plot_pnl( l_alph, fig_name=fname_fig_tcost100, tier=100 )
            plt.close( "all" )
            solbasic.logger.info( "Plotted alphas in [{},{}) as:\n{}\n{}.\n".format( -sim_days_start, -sim_days_end, fname_fig, fname_fig_tcost100 ) )
            cmd = "cp -rp {} {}/pnl/{}/alpha_{}_{}_pool_{}.png".format( fname_fig, work_dir, cfg.sim_period, cfg.mode_neutral, cfg.name_batch, str_today ).split()
            sp = subprocess.call( cmd )
#            cmd = "cp -rp {} {}/pnl/{}/alpha_{}_{}_pool_{}.png".format( fname_fig_tcost100, work_dir, cfg.sim_period, cfg.mode_neutral, cfg.name_batch, str_today ).split()
#            sp = subprocess.call( cmd )
    solbasic.logger.info( "Simulation finished." )
