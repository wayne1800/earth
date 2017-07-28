
""" alpha util functions """

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
import subprocess
from collections import Counter
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import multiprocessing as mp
import numpy as np
import pandas as pd
from sol.finance import finance
from pandas import DataFrame, Series, Panel, HDFStore
from sol.core import basic as solbasic
from PyTools.util import utils
from earth.conf import cfg
from earth.conf import glb
from earth.mod import base
from earth.pnl import pnl
from earth.mod import gicsg
from earth.mod import rvg
from earth.mod import test
from earth.alpha import ops


def get_fields_used( expr_alpha ):
    """ get dictionary module:list of fields used in alpha expression """
    l_groups = re.findall( r'([\w_]+.[\w_]+)', expr_alpha )
    l_groups = [ x for x in l_groups if not x.replace( '.', '' ).isdigit() ] # remove numbers
    d = {}
    for group in l_groups:
        module, field = group.split('.')
        if module.isdigit() or module in [ "ops" ]:
            continue
        if module in d.keys():
            d[ module ].append( field )
        else:
            d[ module ] = [ field ]
    return d


def get_ir( v_pnl ):
    """ get daily ir """
    test.v_pnl = v_pnl
    return v_pnl[ pd.notnull( v_pnl ) ].mean() / v_pnl[ pd.notnull( v_pnl ) ].std()


def get_ir_yearly( v_pnl, n_years, n_days, verbose=True ):
    """ get ir from daily pnl series """
    l_pnl_yearly = []
    sim_start_pnl = len( v_pnl )
    test.sim_start_pnl = sim_start_pnl
    for i in range( n_years ):
        test.i = i
        if i == n_years - 1:
            l_pnl_yearly.append( get_ir( v_pnl.ix[ -sim_start_pnl+n_days*i: ] ) )
        else:
            test.v_pnl = v_pnl.copy()
            l_pnl_yearly.append( get_ir( v_pnl.ix[ -sim_start_pnl+n_days*i:-sim_start_pnl+n_days*( i+1 ) ] ) )
    ser_pnl_yearly = pd.Series( l_pnl_yearly )
    mean_pnl_yearly = ser_pnl_yearly.mean()
    std_pnl_yearly = ser_pnl_yearly.std()
    if np.isnan( std_pnl_yearly ):
        if verbose:
            solbasic.logger.warn( "std_pnl_yearly is Nan, set 1." )
        std_pnl_yearly = 1
    if np.isnan( mean_pnl_yearly ):
        solbasic.logger.warn( "mean_pnl_yearly and ir_yearly is Nan. Keep them as is." )
    return mean_pnl_yearly / std_pnl_yearly


def max_dd( ser ):
    """ compute max drawdown and length from cumulative pnl series """
    test.ser = ser.copy()
    max2here = ser[ pd.notnull( ser ) ].expanding( min_periods=1 ).max()
    dd2here = ser[ pd.notnull( ser ) ] - max2here
    edate = dd2here.idxmin()
    sdate = ( dd2here[ :edate ] == 0 ).cumsum().idxmax()
    mdd_length = dd2here.ix[ sdate : edate ].shape[0]
    mdd_dates = "{}-{}".format( datetime.datetime.strftime( sdate, "%Y%m%d" ), datetime.datetime.strftime( edate, "%Y%m%d" ) )
    return -dd2here.min(), mdd_length, mdd_dates, sdate, edate


def dump_stats( alph_full, tree=None, b_dump_corr=False, b_dump_pos_only=None, b_recreate_alpha=None, book_size=2e7, b_update_db=False ):
    """ dump alpha statistics to alph.path. Must stay in lock while writing 
    """
    b_dump_pos_only = solbasic.sync_mod_attr( b_dump_pos_only, cfg, "b_dump_pos_only" )
    b_recreate_alpha = solbasic.sync_mod_attr( b_recreate_alpha, cfg, "b_recreate_alpha" )
    if cfg.b_dryrun:
        solbasic.logger.info( "Dry run. No dump_stats write." )
        return
    if not hasattr( alph_full, "path" ):
        solbasic.logger.warn( "Alpha path does not exist, unable to dump stats." )
        return
    if not os.path.exists( alph_full.path ):
        os.makedirs( alph_full.path )
    alph = copy.deepcopy( alph_full )

    alph = alph.simplify( mode="shallow" )
    # dump daily position
    alph.mat_raw.index = pd.to_datetime( alph.mat_raw.index )
    alph.mat.index = pd.to_datetime( alph.mat.index )
    base.close.index = pd.to_datetime( base.close.index )
    if not cfg.b_skip_daily_pos:
        path_alpha = "{}alpha/".format( alph.path )
        path_alpha_10by10 = "{}alpha_10by10/".format( alph.path )
        if not os.path.exists( path_alpha ):
            os.makedirs( path_alpha )
        if not os.path.exists( path_alpha_10by10 ):
            os.makedirs( path_alpha_10by10 )
        for index in alph.mat_raw.ix[ alph.d_configs[ "start_date_alpha" ]:alph.d_configs[ "end_date_alpha" ] ].index:
            index = str( index ).split()[0].replace( '-', '' )
            path_alpha_i = "{}{}".format( path_alpha, index )
            path_alpha_i_10by10 = "{}{}".format( path_alpha_10by10, index )
            if not ( os.path.exists( path_alpha_i ) and os.path.exists( path_alpha_i_10by10 ) ) or b_recreate_alpha:
                if not os.path.exists( path_alpha_i ) and cfg.b_refresh_cache:
                    solbasic.logger.debug( "Refreshing {} ...".format( index ) )
                alph.mat.ix[ index ].fillna(0).to_csv( path_alpha_i, sep=' ' )
                if index in base.close.index:
                    np.round( alph.mat.ix[ index ] * book_size / base.close.ix[ index ] ).fillna(0).astype( int ).to_csv( path_alpha_i_10by10, sep=' ' )

        # dump full alpha stats
        if not b_dump_pos_only:
            path_pnl = "{}pnl.csv".format( alph.path ) 
            path_tvr = "{}tvr.csv".format( alph.path ) 
            path_plot = "{}pnl.png".format( alph.path )
            path_config = "{}config.yml".format( alph.path )
            with open( path_config, 'w' ) as outfile:
                outfile.write( yaml.dump( alph.d_configs, default_flow_style=True ) )
            alph.v_pnl.to_csv( path_pnl )
            if hasattr( alph, "v_pnl25" ):
                alph.v_pnl25.to_csv(  path_pnl.replace( "pnl.csv", "pnl25.csv" ) )
                alph.v_pnl50.to_csv(  path_pnl.replace( "pnl.csv", "pnl50.csv" ) )
                alph.v_pnl100.to_csv( path_pnl.replace( "pnl.csv", "pnl100.csv" ) )
            ( np.abs( alph.mat - alph.mat.shift(1) ) ).sum( axis=1 ).to_csv( path_tvr )
            plot_pnl( [ alph ], fig_name=path_plot, verbose=False )

        path_alph = "{}alp".format( alph.path )
        if tree: # alpha cache
            cp_alph = copy.deepcopy( dict( pop_init=[ tree ], l_alph=[ alph ] ) )
            pickle.dump( cp_alph, open( path_alph, 'wb' ), -1 )
        else: # combo cache
            cp_alph = copy.deepcopy( dict( l_alph=[ alph ] ) )
            pickle.dump( cp_alph, open( path_alph, 'wb' ), -1 )


def get_dates_alpha( alph ):
    """ convert simulation days (int) to str dates """
    start_date_alpha = str( alph.mat_raw.index[ -alph.d_configs[ "sim_days_start" ] ] ).split()[0].replace( '-', '' )
    is_vs_date_alpha = str( alph.mat_raw.index[ -alph.d_configs[ "sim_days_is_vs" ] ] ).split()[0].replace( '-', '' )
    end_date_alpha   = str( alph.mat_raw.index[ -alph.d_configs[ "sim_days_end" ] ] ).split()[0].replace( '-', '' )
    return start_date_alpha, end_date_alpha, is_vs_date_alpha


def get_prod_start_date( alph ):
    """ get alpha prod start date from earliest position timestamp """
    prod_start_date = ''
    if os.path.exists( alph.path ):
        try:
            time_earliest_pos = min( [ "{}/alpha/{}".format( alph.path, pos ) for pos in os.listdir( "{}/alpha/".format( alph.path ) ) ], key=os.path.getmtime )
            mtime = time.ctime( os.path.getmtime( time_earliest_pos ) )
            prod_start_date = datetime.datetime.strftime( datetime.datetime.strptime( mtime, "%a %b %d %H:%M:%S %Y" ), "%Y%m%d" )
        except:
            solbasic.logger.warn( "Unable to get prod start date from {}. Skip.".format( alph.path ) )
    return prod_start_date


def plot_pnl( l_alph, fig_size=30, fig_name="test.png", tier=0, verbose=True, labelsize=None, start=None, end=None, b_ts_hist=False ):
    """ 
        plot alpha from local positions. 
        b_ts_hist: plot TS performance before production only.
    """
    if cfg.b_dryrun:
        solbasic.logger.info( "Dry run. No pnl plots." )
        return
    n_alphas = len( l_alph )
    if "plot" in cfg.sim_mode:
        solbasic.logger.info( "{} alphas in total.".format( n_alphas ) )
    n_plot_x = n_plot_y = math.ceil( np.sqrt( n_alphas ) )
    if n_alphas > 200:
        fig_size *= math.sqrt( float( n_alphas ) / 300 )
    if labelsize is None:
        labelsize = 12. * 5 / math.sqrt( n_alphas ) 
    params = { 'font.size': labelsize }

    params_ori = { 'font.size': matplotlib.rcParams['font.size'] }
    matplotlib.rcParams.update( params )
    fig = plt.figure( figsize=( fig_size, fig_size*0.618 ) )
    fig.suptitle( cfg.name_batch, fontsize=14, fontweight='bold' )
    if n_plot_x * n_plot_y < n_alphas:
        solbasic.logger.warn( "{} x {} plots cannot hold {} alphas, reduce # alphas.".format( n_plot_x, n_plot_y, n_alphas ) )
    cnt = 0
    if verbose:
        solbasic.logger.info( "\t\t\t\t\tlong\tshort\tir\tpnl\tvlt\ttvr\tmdd" )
    # reorder l_alph by alph.id
    d_id_pos = { alph.id : i for i, alph in enumerate( l_alph ) }
    l_aid = sorted( d_id_pos.keys() )
    for aid in l_aid:
        pos = d_id_pos[ aid ]
        aid_ori = l_alph[ pos ].path.split( '/' )[-2]
        dataset = l_alph[ pos ].path.split( '/' )[-4].split( '_' )[0]
        if os.path.exists( l_alph[ pos ].path ):
            try:
                prod_start_date = get_prod_start_date( l_alph[ pos ] )
            except:
                solbasic.logger.warn( "Unable to get prod start date for alpha {}. Skip.".format( aid ) )

        if b_ts_hist:
            l_alph[ pos ].v_cum_pnl = l_alph[ pos ].v_cum_pnl.ix[ :prod_start_date ]
            if hasattr( l_alph[ pos ], "v_cum_pnl25" ):
                l_alph[ pos ].v_cum_pnl25 = l_alph[ pos ].v_cum_pnl25.ix[ :prod_start_date ]
                l_alph[ pos ].v_cum_pnl50 = l_alph[ pos ].v_cum_pnl50.ix[ :prod_start_date ]
                l_alph[ pos ].v_cum_pnl100 = l_alph[ pos ].v_cum_pnl100.ix[ :prod_start_date ]
        ax_cw = fig.add_subplot( n_plot_x, n_plot_y, cnt + 1 )

        if not start:
            start = 0
        if not end:
            end = -1
        if tier == 0:
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        elif tier == 25 and hasattr( l_alph[ pos ], "v_cum_pnl25" ):
            ( l_alph[ pos ].v_cum_pnl25.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        elif tier == 50 and hasattr( l_alph[ pos ], "v_cum_pnl50" ):
            ( l_alph[ pos ].v_cum_pnl50.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        elif tier == 100 and hasattr( l_alph[ pos ], "v_cum_pnl100" ):
            ( l_alph[ pos ].v_cum_pnl100.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        else:
            solbasic.logger.warn( "Tier unsupported or absent in alpha, plot tier 0 instead." )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        if cfg.sim_mode == "comboplot":
            solbasic.logger.info( "Plotting {} ...".format( l_alph[ pos ].name ) )
        if tier == 0:
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
            if prod_start_date and not l_alph[ pos ].v_cum_pnl.ix[ prod_start_date: ].empty:
                ( l_alph[ pos ].v_cum_pnl.ix[ start : end ] ).ix[ prod_start_date: ].plot( label="prod", ax=ax_cw )
                l_dump_dates = [ datetime.datetime.strftime( ind, "%Y%m%d" ) for ind in l_alph[ pos ].v_pnl.ix[ start : end ].ix[ prod_start_date: ].index ]
                alph_pos_close = get_pnl_pos_close( l_alph[ pos ], l_dump_dates )
                ( alph_pos_close.v_cum_pnl.ix[ start : end ].ix[ prod_start_date: ] ).plot( label="pos", ax=ax_cw )
        elif tier == 25 and hasattr( l_alph[ pos ], "v_cum_pnl25" ):
            ( l_alph[ pos ].v_cum_pnl25.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        elif tier == 50 and hasattr( l_alph[ pos ], "v_cum_pnl50" ):
            ( l_alph[ pos ].v_cum_pnl50.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        elif tier == 100 and hasattr( l_alph[ pos ], "v_cum_pnl100" ):
            ( l_alph[ pos ].v_cum_pnl100.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        else:
            solbasic.logger.warn( "Tier unsupported or absent in alpha, plot tier 0 instead." )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ] ).plot( label="{}_{}_{}: {}".format( dataset, l_alph[ pos ].d_configs[ "mode_neutral" ], aid_ori, l_alph[ pos ].name ), ax=ax_cw )
        if n_alphas < 50: 
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=5, min_periods=5 ).mean().ix[ start : end ] ).plot( label="{}".format( "5" ), legend=False, ax=ax_cw )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=15, min_periods=15 ).mean().ix[ start : end ] ).plot( label="{}".format( "15" ), legend=False, ax=ax_cw )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=30, min_periods=30 ).mean().ix[ start : end ] ).plot( label="{}".format( "30" ), legend=False, ax=ax_cw )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=60, min_periods=60 ).mean().ix[ start : end ] ).plot( label="{}".format( "60" ), legend=False, ax=ax_cw )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=120, min_periods=120 ).mean().ix[ start : end ] ).plot( label="{}".format( "120" ), legend=False, ax=ax_cw )
            ( l_alph[ pos ].v_cum_pnl.ix[ start : end ].rolling( window=240, min_periods=240 ).mean().ix[ start : end ] ).plot( label="{}".format( "240" ), legend=False, ax=ax_cw )
                 
        if verbose:
            if tier == 0:
                solbasic.logger.info( "{}. id={}, {}\n\t\t\t\t\t{}".format( cnt, aid_ori, l_alph[ pos ].expr, l_alph[ pos ].summary ) )
            elif hasattr( l_alph[ pos ], "summary{}".format( tier ) ):
                solbasic.logger.info( "{}. id={}, {}\n\t\t\t\t\t{}".format( cnt, aid_ori, l_alph[ pos ].expr, getattr( l_alph[ pos ], "summary{}".format( tier ) ) ) )
        ax_cw.legend( loc='upper left', prop={ 'size':labelsize }, fancybox=True, framealpha=0 )
        if n_alphas < 50: 
            ax_cw.lines[1].set_linewidth(2)
        cnt += 1
        plt.xlabel( 'date' )
        plt.ylabel( 'pnl' )
        plt.grid( True )

    plt.savefig( fig_name )
    plt.close( fig )
    matplotlib.rcParams.update( params_ori )
