
""" cache util functions """

from __future__ import print_function
import os
import gc
import copy
import shutil
import datetime
import pickle
from sys import modules
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
from sol.core import basic as solbasic
from earth.conf import cfg
from earth.conf import glb
from earth.alpha import ops
from earth.mod import base
from earth.mod import adj
from earth.mod import test


def load_stats_aid( path ):
    " load alpha ids from stats cache """
    if not os.path.exists( path ):
        solbasic.logger.warn ( "{} does not exist, skip.".format( path ) )
        return []
    cp = pickle.load( open( path, 'r' ) )
    l_alph = cp[ 'l_alph' ]
    l_aid = [ alph.id for alph in l_alph ]
    return l_aid


def load_stats_aid_dir( path ):
    " load alpha ids from path=*/pool """
    if not os.path.exists( path ):
        solbasic.logger.warn ( "{} does not exist, skip.".format( path ) )
        return []
    l_path = solbasic.get_subdir( "{}/".format( path ) )
    l_aid = [ int( path.split( '/' )[-1] ) for path in l_path ]
    return l_aid


def load_stats_cache( path, verbose=True, l_aid=None, b_path_is_symlink=False, b_return_smart=True ):
    """ 
        load alpha stats cache from path 
        l_aid: alpha ids to load
        b_path_is_symlink: decide alpha id by path. Needed if path is a symlink
    """
    if not os.path.exists( path ):
        if verbose: 
            solbasic.logger.warn ( "{} does not exist, skip.".format( path ) )
        return
    cp = pickle.load( open( path, 'r' ) )
    l_alph = copy.deepcopy( cp[ 'l_alph' ] )
    l_alph_new = []
    set_pop_init_index = set([])
    if b_path_is_symlink:
        for i in range( len( l_alph ) ):
            l_alph[i].id = int( path.split( '/' )[-2] )
    for i in range( len( l_alph ) ):
        l_alph[i] = l_alph[i].simplify( mode="shallow" )
    if l_aid and type( l_aid ) is list:
        for i, alph in enumerate( l_alph ):
            if alph.id in l_aid:
                l_alph_new.append( alph )
                set_pop_init_index.add( i )
        l_alph = l_alph_new
    if "pop_init" in cp.keys():
        if set_pop_init_index:
            pop_init = [ x for i, x in enumerate( copy.deepcopy( cp[ 'pop_init' ] ) ) if i in set_pop_init_index ]
        else:
            pop_init = copy.deepcopy( cp[ 'pop_init' ] )
        if len( pop_init ) != len( l_alph ) and verbose:
            solbasic.logger.warn( "pop_init ({}) and l_alph ({}) have different sizes.".format( len( pop_init ), len( l_alph ) ) )
        if verbose: 
            solbasic.logger.info( "Loaded pop_init, l_alph from {}, {} alphas in total.".format( path, len( pop_init ) ) )
        if b_return_smart and len( l_alph ) == 1:
            return l_alph[0]
        else:
            return cp, pop_init, l_alph
    else:
        if verbose: 
            solbasic.logger.info( "Loaded l_alph from {}, {} alphas in total.".format( path, len( l_alph ) ) )
        if b_return_smart and len( l_alph ) == 1:
            return l_alph[0]
        else:
            return cp, l_alph


def load_stats_cache_dirs( str_alpha_position, l_aid=None, verbose=False ):
    " load alpha stats cache from pool dir """
    l_alph = []
    l_alph_dir = solbasic.get_subdir( str_alpha_position )
    set_aid = set([])
    if l_aid:
        l_aid = list( set( l_aid ) & set( [ int( os.path.split( adir )[-1] ) for adir in l_alph_dir ] ) )
        set_aid = set( l_aid )
    else:
        l_aid = list( set( [ int( os.path.split( adir )[-1] ) for adir in l_alph_dir ] ) )
    n_alphas = len( l_aid )
    l_aid_final = []
    cnt = 0
    for adir in l_alph_dir:
        aid = int( os.path.split( adir )[-1] )
        if set_aid and aid not in set_aid:
            continue
        l_alph_ind = load_stats_cache( "{}/alp".format( adir ), verbose=verbose )[-1]
        alph = l_alph_ind[0]
        if alph.id != aid and verbose:
            if not ( cfg.sim_mode == "alpharef" and alph.id == aid + cfg.n_id_increment ):
                solbasic.logger.warn( "Alpha id {} does not match alpha folder {}, skip.".format( alph.id, aid ) )
                continue
        elif alph.mat_raw.dropna( how='all' ).empty and verbose:
            solbasic.logger.warn( "Alpha {}: {} empty, skip.".format( alph.id, alph.expr_raw ) )
            continue
        if cfg.b_pnl_cov_only:
            l_alph_ind[0].mat_raw = DataFrame() 
        l_aid_final.append( alph.id )
        l_alph.extend( copy.deepcopy( l_alph_ind ) )
        report_progress( cnt, n_alphas, "stats caches loaded from {} ".format( str_alpha_position ) )
        cnt += 1
    return [ l_aid_final, l_alph ]


def create_cache( module, n_days_refresh=None ):
    """ create all matrices in module to data cache """
    if cfg.b_dryrun:
        solbasic.logger.info( "Dry run. No create_cache write." )
        return
    if not os.path.exists( cfg.path_cache ):
        os.makedirs( cfg.path_cache )
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    if cfg.b_refresh_cache:
        n_days_refresh = solbasic.sync_mod_attr( n_days_refresh, cfg, "n_days_refresh" )
        solbasic.logger.info( "Refreshing {} ...".format( path_store ) )
        append_store_mod( module, path_store, n_days_refresh=n_days_refresh )
    else:
        solbasic.logger.info( "Creating {} ...".format( path_store ) )
        remove_cache( module, verbose=False )
        save_store_mod( module, path_store )


def append_cache( module, field ):
    """ append module.field to data cache """
    if cfg.b_dryrun:
        solbasic.logger.info( "Dry run. No append_cache write." )
        return
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    if not os.path.exists( cfg.path_cache ) or not os.path.exists( path_store ):
        solbasic.logger.warn( "{} absent, set b_recreate_store = 1 and try again.".format( path_store ) )
        return
    else:
        solbasic.logger.info( "Appending {}.{} to {} ...".format( module.__name__.split('.')[-1], field, path_store ) )
        save_store_field( module, field, path_store )


def load_cache_interactive( module ):
    """ load store """ 
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    return HDFStore( path_store )


def load_cache( module, l_fields=None ):
    """ load all matrices in store to module """ 
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    with pd.get_store( path_store ) as store:
        for field in store.keys():
            field_name = field.replace( '/', '' )
            if ( l_fields == None or field_name in l_fields ):
                if cfg.sim_days_end == 1:
                    setattr( module, field_name, store[ field ] )
                else:
                    setattr( module, field_name, store[ field ].ix[ : -cfg.sim_days_end ] )


def exists_cache( module ):
    """ check if the cache for the module exists """
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    if os.path.exists( path_store ):
        return True
    else:
        return False


def hasattr_cache( module, field ):
    """ check if the cache for the module has field """
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    if not os.path.exists( path_store ):
        solbasic.logger.warn( "{} does not exist. No attribute.".format( path_store ) )
        return False
    b_hasattr = False
    with pd.get_store( path_store ) as store:
        if "/{}".format( field ) in store.keys():
            b_hasattr = True
    return b_hasattr


def getattr_cache( module, field ):
    """ check if the cache for the module has field, and get it if so """
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    attr = DataFrame()
    if not os.path.exists( path_store ):
        solbasic.logger.warn( "{} does not exist. No attribute.".format( path_store ) )
    else:
        with pd.get_store( path_store ) as store:
            if "/{}".format( field ) in store.keys():
                attr = store[ field ]
    return attr


def remove_cache( module, verbose=True ):
    """ remove the cache for the module """
    if cfg.sim_period in [ "forward", "live" ]:
        path_store = "{}{}_{}_forward.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date )
    else:
        path_store = "{}{}_{}_{}.h5".format( cfg.path_cache, module.__name__.split('.')[-1], cfg.start_date, cfg.end_date )
    if os.path.exists( path_store ):
        os.unlink( path_store )
        if verbose:
            solbasic.logger.debug( "Removed {}.".format( path_store ) )


def adjust_mod( l_fields, module, l_except=None, mode='*' ):
    """ add adjusted per share stats by adj.adjcs in module """
    for field in l_fields:
        if field in dir( module ) and ( not l_except or field not in l_except ):
            if mode == '*':
                setattr( module, field + "_adj", getattr( module, field ) * adj.adjcs )
            elif mode == '/':
                setattr( module, field + "_adj", getattr( module, field ) / adj.adjcs )
            else:
                solbasic.logger.warn( "Adjust mode {} unsupported, skip.".format( mode ) )
        elif l_except and field in l_except:
            solbasic.logger.info( "{} excluded.".format( field ) )
        else:
            solbasic.logger.warn( "{} not in {}.".format( field, module.__name__.split( '.' )[-1] ) )


def normalize_mod( l_fields, source, target ):
    """ normalize stats by cap for peer comparison. Get fields from module and save to target """
    if not hasattr( base, "cap" ):
        solbasic.logger.warn( "Unable to normalize since base.cap is undefined." )
        return
    for field in l_fields:
        if field in dir( source ) and type( getattr( source, field ) ) is DataFrame:
            try:
                setattr( target, field, getattr( source, field ) / base.cap )
            except TypeError:
                solbasic.logger.warn( "TypeError: {} is not a DataFrame.".format( field ) )
                solbasic.logger.warn( getattr( source, field ) )
        else:
            solbasic.logger.warn( "{} not in {}.".format( field, source.__name__.split( '.' )[-1] ) )


def ffill_mod( source, target, b_itsn=True ):
    """ front fill all DataFrames in source and save to target module
    b_itsn: truncate dates to price series
    """
    for df in dir( source ):
        if type( getattr( source, df ) ) is DataFrame:
            if b_itsn and hasattr( base, "close" ):
                setattr( target, df, getattr( source, df ).fillna( method='ffill' ).ix[ base.close.index ] )
            else:
                setattr( target, df, getattr( source, df ).fillna( method='ffill' ) )


def recover_ffill( source, target ):                                                           
    """ recover front filled DataFrames in source and save to target module. Inverse operation of ffill_mod """
    for df in dir( source ):
        if type( getattr( source, df ) ) is DataFrame:
            df_diff = getattr( source, df ).diff()                                             
            df_recovered = getattr( source, df )[ df_diff != 0 ]
            setattr( target, df, df_recovered )
            
            
def set_mod_chg( source, target ):
    """ get daily change for all DataFrames in source and save to target module. default ffill in pct calculation. """
    for df in dir( source ):
        if type( getattr( source, df ) ) is DataFrame:
            setattr( target, df, ( getattr( source, df ) - getattr( source, df ).shift(1) ) )


def set_mod_pct_chg( source, target, l_except=None, b_itsn=True ):
    """ get percent change for all DataFrames in source and save to target module. default ffill in pct calculation. """
    for df in dir( source ):
        if type( getattr( source, df ) ) is DataFrame and ( not l_except or df not in l_except ):
            # flip sign if initial value is negetive
            if b_itsn and hasattr( base, "close" ):
                setattr( target, df, ( getattr( source, df ).pct_change() * np.sign( getattr( source, df ).shift(1).fillna( method='ffill' ) ) ).fillna( method='ffill' ).ix[ base.close.index ] * 100 )
            else:
                setattr( target, df, ( getattr( source, df ).pct_change() * np.sign( getattr( source, df ).shift(1).fillna( method='ffill' ) ) ).fillna( method='ffill' ) * 100 )
        elif l_except and df in l_except:
            solbasic.logger.info( "{} excluded.".format( df ) )


def set_mod_pct_chg_annual( source, target, ind='tdate', l_except=None,  b_itsn=True ):
    """ get percent change from same time last year for all DataFrames in source and save to target module """
    y_str = lambda x: str( int( str(x).split('-')[0] ) + 1 ) + '-' + '-'.join( str(x).split('-')[1:] )
    y_dt = lambda x: datetime.datetime.strptime( str( int( str(x).split('-')[0] ) + 1 ) + '-' + '-'.join( str(x).split('-')[1:] ).split()[0], "%Y-%m-%d" )
    dt_null = datetime.datetime.strptime( "19000101", "%Y%m%d" )
    for field in dir( source ):
        if type( getattr( source, field ) ) is DataFrame and ( not l_except or field not in l_except ):
            df_shift_1yr = getattr( source, field ).copy()
            df_shift_1yr[ "index_shift" ] = [ y_dt(x) if "-02-29" not in str(x) else dt_null for x in df_shift_1yr.index ]
            df_shift_1yr = df_shift_1yr[ df_shift_1yr[ "index_shift" ] != dt_null ]
            df_shift_1yr.set_index( "index_shift", inplace=True )
            df_shift_1yr.index.rename( ind, inplace=True )
            df_shift_1yr = df_shift_1yr.ix[ getattr( source, field ).index.union( df_shift_1yr.index ) ]
            df_shift_1yr = df_shift_1yr.fillna( method='ffill' )
            df_shift_1yr = df_shift_1yr.ix[ getattr( source, field ).index ]
            test.df_shift_1yr = df_shift_1yr
            test.df_test = getattr( source, field )
            if b_itsn and hasattr( base, "close" ):
                setattr( target, field, ( ( getattr( source, field ) - df_shift_1yr ) / df_shift_1yr * np.sign( df_shift_1yr ) ).ix[ base.close.index ] * 100 )
            else:
                setattr( target, field, ( getattr( source, field ) - df_shift_1yr ) / df_shift_1yr * np.sign( df_shift_1yr ) * 100 )
        elif l_except and field in l_except:
            solbasic.logger.info( "{} excluded.".format( field ) )


def clear_mod( module ):
    """ clean up all contents in module """
    for df in dir( module ):
        if type( getattr( module, df ) ) is DataFrame:
            del vars( module )[ df ]


def rank_mod( source, target ):
    """ get rank for all DataFrames in source and save to target module. """
    for df in dir( source ):
        if type( getattr( source, df ) ) is DataFrame:
            setattr( target, df, ops.rankM( getattr( source, df ) ) )


def add_mod_dict( d, module, l_except=None ):
    """ add all fields in dictionary to module """ 
    for field, data in d.iteritems():
        if not l_except or field not in l_except:
            if type( data ) is DataFrame and not data.empty:
                setattr( module, field, data )
            else:
                solbasic.logger.warn( "{} is not a matrix or is empty, skip.".format( field ) )
        elif l_except and field in l_except:
            solbasic.logger.info( "{} excluded.".format( field ) )


def add_mod_df( df, module, l_except=None ):
    """ add all fields in DataFrame to module """ 
    for field in df.columns.get_level_values(0).unique():
        if not l_except or field not in l_except:
            setattr( module, field, getattr( df, field ) )
        elif l_except and field in l_except:
            solbasic.logger.info( "{} excluded.".format( field ) )


def move_mod( source, target, l_fields=None, prefix=False ):
    """ move matrices in source module to target. Move all matrices if fields unspecified """
    if l_fields == None:
        l_fields = [ x for x in vars( source ).keys() if type( vars( source )[x] ) is DataFrame ]
    for field in l_fields:
        if prefix:
            field_prefixed = source.__name__.split('.')[-1] + "_" + field
        setattr( target, field_prefixed, getattr( source, field ) )
        del vars( source )[ field ]


def mul_mod_const( module, l_fields, const=100 ):
    """ multiply specified fields in module by const """
    for field in l_fields:
        if hasattr( module, field ):
            vars( module )[ field ] *= 100


def save_store_mod( module, path_store ):
    """ save all fields in module to store """ 
    for field in module.__dict__.keys():
        if type( getattr( module, field ) ) is DataFrame or type( getattr( module, field ) ) is Panel:
            getattr( module, field ).to_hdf( path_store, field, mode='a', format='fixed' )


def append_store_mod( module, path_store, n_days_refresh=None, b_ptrk=False ):
    """ append all new rows in module.field to store. Resize store as appropriate. """ 
    store = HDFStore( path_store )
    for field in module.__dict__.keys():
        if ( type( getattr( module, field ) ) is DataFrame or type( getattr( module, field ) ) is Panel ) and "/{}".format( field ) in store.keys():
            if "tdate" in field:
                getattr( module, field ).to_hdf( path_store, field, mode='a', format='fixed' )
            else:
                solbasic.logger.info( "Working on {}...".format( field ) )
                df = store[ field ].copy()
                df_new = getattr( module, field ).copy()
                if n_days_refresh == None:
                    l_index = sorted( list( set( df_new.index ) - set( df.index ) ) )
                else:
                    l_index = sorted( list( df_new.index[ -n_days_refresh: ] ) )
                l_columns = sorted( list( set( df_new.columns ) - set( df.columns ) ) )
                l_columns_rev = sorted( list( set( df.columns ) - set( df_new.columns ) ) )
                if l_columns:
                    solbasic.logger.info( "Adding {} instruments: {}".format( len( l_columns ), l_columns ) )
                    for col in l_columns:
                        df[ col ] = np.nan
                if l_columns_rev:
                    for col in l_columns_rev:
                        df_new[ col ] = df[ col ]
                if l_index:
                    solbasic.logger.info( "Refreshing {} dates: {}".format( len( l_index ), l_index ) )
                    for ind in l_index:
                        df.ix[ ind ] = df_new.ix[ ind ]
                    df.to_hdf( path_store, field, mode='a', format='fixed' )
    store.close()
    if b_ptrk:
        ptrk_store( path_store )


def save_store_field( module, field, path_store ):
    """ append df to store """ 
    df = getattr( module, field )
    if type( df ) is DataFrame:
        df.to_hdf( path_store, field, mode='a', format='fixed' )


def get_mod_pct_list( module ):
    """ find all matrices with pct unit and return list of strings """
    l_fields_pct = []
    for field in vars( module ).keys():
        df = getattr( module, field )
        if type( df ) is DataFrame:
            f_max = df.max().max()
            f_min = df.min().min()
            if f_min > 0 and f_min < 20 and f_max < 100 and f_max > 80:
                l_fields_pct.append( field )
    return l_fields_pct
