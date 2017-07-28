
""" universe construction and module preparation """

import sys
import numpy as np
import copy
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
from sol.cnx import sql
from sol.core import basic as solbasic
from earth.conf import cfg
from earth.conf import glb
from earth.cache import dmgr
from earth.cache import cutils
from earth.mod import univ
from earth.mod import base
from earth.mod import adj
from earth.mod import base_etf
from earth.pnl import pnl
from earth.pnl import pnl_etf
from earth.mod import cs
from earth.mod import csf
from earth.mod import csn
from earth.mod import csq
from earth.mod import csa
from earth.mod import ciq
from earth.mod import ciqf
from earth.mod import ciqn
from earth.mod import ciqq
from earth.mod import ciqa
from earth.mod import ws
from earth.mod import wsf
from earth.mod import wsn
from earth.mod import wsq
from earth.mod import wsa
from earth.mod import gicsg
from earth.mod import rvg
from earth.mod import test

class preparer:
    """ class to prepare matrices for alpha use """

    def __init__( self, iden=None ):

        self.iden = solbasic.sync_mod_attr( iden, cfg, "iden_stock" )
	self.data_mgr = dmgr.dmgr()
        if cfg.db in [ "xf", "qa", "fs" ]:
            self.conn = sql.sqllink( cfg.db, "sqlalchemy" )
        self.select_run_mode()


    def exec_top( self, func_dmgr, args=[], mode="sqlserver", b_union=False, b_itsn=False, tdate="tdate", l_duplicate=None ):
        """ 
            execute dmgr, load data, truncate to top universe and pivot 
            mode: sqlserver, sqlite3, raw. 
        """
        iden = self.iden 
        if mode in [ "sqlserver" ]: # load from sql server
            if not hasattr( self, "conn" ):
                self.conn = sql.sqllink( cfg.db, "sqlalchemy" )
            if args:
                args.insert( 0, self.conn )
                mat = func_dmgr( *args )
            else:
                mat = func_dmgr( self.conn )
        elif mode == "raw": # load from raw data
            if type( func_dmgr ) is DataFrame:
                mat = func_dmgr
            elif callable( func_dmgr ): 
                if args:
                    mat = func_dmgr( *args )
                else:
                    mat = func_dmgr()
            else:
                solbasic.logger.critical( "Unable to recognize func_dmgr type {}.".format( type( func_dmgr ) ) )
        else:
            solbasic.logger.warn( "exec_top mode {} not supported. Available modes: sqlserver, sqlite3, raw.".format( mode ) )
            mat = DataFrame()
        if mat.empty:
            return mat
        index_itsn = set( mat[ iden ] ).intersection( set( univ.mat_in_play.columns ) )
        test.mat1 = mat.copy()

        # drop duplicates for fundamental data. fyr changes but values the same
        if l_duplicate:
            mat = mat.set_index( iden ).ix[ index_itsn ].reset_index( level=0 ).drop_duplicates( subset = [ tdate, iden ] ).drop_duplicates( subset = l_duplicate ).pivot( index=tdate, columns=iden )
        else:
            mat = mat.set_index( iden ).ix[ index_itsn ].reset_index( level=0 ).drop_duplicates( subset = [ tdate, iden ] ).pivot( index=tdate, columns=iden )
        if b_union and hasattr( base, "close" ):
            mat = DataFrame( mat, index=base.close.index.union( mat.index ) )
        elif b_itsn and hasattr( base, "close" ):
            mat = DataFrame( mat, index=base.close.index.intersection( mat.index ) )
        test.mat2 = mat.copy()
        return mat

    
    def select_top_univ( self, mat_to_rank, n_in_play ):
        """ compute top N universe by given metric, set inactive instruments NaN """
        mat_rank = mat_to_rank.rank( axis=1, ascending=False )
        mat_in_play = -np.sign( mat_rank - n_in_play - 1 )
        mat_in_play[ mat_in_play <= 0 ] = np.nan
        mat_in_play = mat_in_play.dropna( how='all', axis=1 )
        return mat_in_play
    
    
    def select_run_mode( self ):
        """ set top universe in play """
        if cfg.b_recreate_cache or not cutils.exists_cache( univ ):
            df_in_play = self.data_mgr.load_univ( self.conn )
            #univ.mat_reference = df_in_play[ [ "gvkey", "tdate", "isin", "cusip", "sedol", "ticker" ] ].copy()  #big table and takes too much time
            #univ.mat_reference = df_in_play[ [ "tdate", self.iden, "ticker" ] ].copy()
            test.df_in_play = df_in_play.copy()
            df_in_play = df_in_play.drop_duplicates( subset=[ 'tdate', self.iden ] ).pivot( index="tdate", columns=self.iden )
            mat_volume_ffill = df_in_play.volume.fillna( method='ffill', limit=10 )
            mat_close_ffill = df_in_play.close.fillna( method='ffill', limit=10 )
            mat_close_ffill_ewma60 = mat_close_ffill.ewm( span=60 ).mean()
            mat_liquidity = ( mat_volume_ffill * mat_close_ffill ).fillna(0)
            mat_liquidity_ewma60 = mat_liquidity.ewm( span=60 ).mean()
            # remove stocks outside price range
            mat_liquidity_ewma60[ ( mat_close_ffill_ewma60 < cfg.f_price_lower ) | ( mat_close_ffill_ewma60 > cfg.f_price_upper ) ] = np.nan
            for n_in_play in cfg.l_n_in_play:
                if cfg.n_in_play >= n_in_play:
                    setattr( univ, "mat_in_play_{}".format( n_in_play ), self.select_top_univ( mat_liquidity_ewma60, n_in_play ) )
            self.mat_in_play = getattr( univ, "mat_in_play_{}".format( cfg.n_in_play ) )
            self.mat_in_play.ix[0] = self.mat_in_play.ix[1]
            self.mat_in_play = self.mat_in_play.shift( cfg.delay )
            solbasic.logger.info( "All universe determined: {} days, {} instruments.".format( df_in_play.shape[0], df_in_play.close.shape[1] ) )
	    self.mat_in_play = self.mat_in_play.dropna( how='all', axis=1 )
            univ.mat_in_play = self.mat_in_play
            cutils.create_cache( univ )
        else:
            solbasic.logger.info( "Loading top{} universe from cache...".format( cfg.n_in_play ) )
            cutils.load_cache( univ )
            if cutils.hasattr_cache( univ, "mat_in_play_{}".format( cfg.n_in_play ) ):
                univ.mat_in_play = cutils.getattr_cache( univ, "mat_in_play_{}".format( cfg.n_in_play ) ).dropna( how='all', axis=1 ).shift( cfg.delay )
            else:
                solbasic.logger.critical( "mat_in_play absent in univ cache, exit." )
                sys.exit(1)

        if cfg.sim_period in [ "forward", "live" ]:
            cfg.sim_days_start = univ.mat_in_play.ix[ "20140101": ].shape[0]
            cfg.d_configs[ "sim_days_start" ] = cfg.sim_days_start
            cfg.sim_days_is_vs = cfg.sim_days_start / 3
            cfg.d_configs[ "sim_days_is_vs" ] = cfg.d_configs[ "sim_days_start" ]
        glb.str_in_play = "'" + "','".join( univ.mat_in_play.columns.astype( str ) ) + "'"
        solbasic.logger.info( "Top {} universe determined: {} days, {} instruments.".format( cfg.n_in_play, univ.mat_in_play.shape[0], univ.mat_in_play.shape[1] ) )
    

    def base_data( self, d_fields_used={}, load_type="base", n_days_refresh=None, verbose=False ):
        """ prepare base data, returns, costs 
        d_fields_used: fields to load if specified.
        load_type: base: everything. pnl: only pnl module.
        verbose: if b_prepare_data False, print more information on selective cache loading
        """
        if cfg.b_recreate_cache or cfg.b_refresh_cache or not cutils.exists_cache( adj ):
            solbasic.logger.info( "Building adjustment factors..." )
            df_adj = self.exec_top( self.data_mgr.load_adj )
            test.df_adj = df_adj.copy()
            adj.adjdi = df_adj.adjdi.div( df_adj.adjdi[ df_adj.adjdi.lastobs() ].bfill() )
            adj.adjcs = df_adj.adjcs.div( df_adj.adjcs[ df_adj.adjcs.lastobs() ].bfill() )
            cutils.create_cache( adj )
        else:
            cutils.load_cache( adj )
            solbasic.logger.info( "Adjustment factors loaded: {} days, {} instruments.".format( adj.adjcs.shape[0], adj.adjcs.shape[1] ) )

        if cfg.b_recreate_cache or cfg.b_refresh_cache or not cutils.exists_cache( base ) or not cutils.exists_cache( pnl ):
            if cfg.b_recreate_cache:
                solbasic.logger.info( "Building base data..." )
            elif cfg.b_refresh_cache and n_days_refresh != None:
                solbasic.logger.info( "Refreshing last {} days of base data...".format( n_days_refresh ) )
            else:
                solbasic.logger.info( "Building base data..." )

            df_daily = self.exec_top( self.data_mgr.load_prices )
            test.df_daily = df_daily.copy()
            if df_daily.empty:
                solbasic.logger.error( "Base data failed to load." )
                return
            base.open = df_daily.open
            base.high = df_daily.high
            base.low = df_daily.low
            base.close = df_daily.close
            base.volume = df_daily.volume
            base.sharesout = df_daily.sharesout
            base.cap = base.close * base.sharesout / 1e6 #in million
            base.gap = base.open - base.close.shift(1)
            pnl.tcost50 = DataFrame( cfg.tcost50, base.close.index, base.close.columns )
            pnl.tcost50 += 0.0002 # tcost buffer for fluctuation
            pnl.tcost100 = 2 * pnl.tcost50
            pnl.tcost25 = 0.5 * pnl.tcost50

            base.open_adj = base.open / adj.adjcs
            base.open_adj = base.open_adj.replace( [ np.inf, -np.inf ], np.nan )
            base.close_adj = base.close / adj.adjcs
            base.close_adj = base.close_adj.replace( [ np.inf, -np.inf ], np.nan )
            base.high_adj = base.high / adj.adjcs
            base.high_adj = base.high_adj.replace( [ np.inf, -np.inf ], np.nan )
            base.low_adj = base.low / adj.adjcs
            base.low_adj = base.low_adj.replace( [ np.inf, -np.inf ], np.nan )
            base.volume_adj = base.volume * adj.adjcs
            base.volume_adj = base.volume_adj.replace( [ np.inf, -np.inf ], np.nan )
            base.close_adj_std30 = base.close_adj.rolling( window=30, min_periods=0 ).std() / base.close_adj
            base.close_adj_autocorr5 = base.close_adj.rolling( window=5, min_periods=5 ).corr( base.close_adj.shift(1) ) * 100
            base.close_adj_sgn_autocorr5 = np.sign( base.close_adj_autocorr5 ) * 100
            base.close_adj_autocorr30 = base.close_adj.rolling( window=30, min_periods=10 ).corr( base.close_adj.shift(1) ) * 100
            base.close_adj_sgn_autocorr30 = np.sign( base.close_adj_autocorr30 ) * 100
#            base.vwap_adj = base.vwap * adj.adjdi
            base.returns = base.close_adj.pct_change( fill_method=None ) * 100
            base.sgn_returns = np.sign( base.returns ) * 100
            base.returns5 = base.returns.rolling( window=5, min_periods=0 ).mean()
            base.returns15 = base.returns.rolling( window=15, min_periods=0 ).mean()
            base.returns30 = base.returns.rolling( window=30, min_periods=0 ).mean()
            pnl.ret = base.close_adj.pct_change( fill_method=None )
            pnl.ret_open = base.open_adj.pct_change( fill_method=None )
#            pnl.ret_vwap_entry = ( base.close_adj - base.vwap_adj ) / base.vwap_adj
#            pnl.ret_vwap_exit = ( base.vwap_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)
            pnl.ret_high_exit = ( base.high_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)
            pnl.ret_low_exit = ( base.low_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)
            pnl.ret_open_entry = ( base.close_adj - base.open_adj ) / base.open_adj
            pnl.ret_open_exit = ( base.open_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)
            pnl.ret_open_high_entry = ( base.high_adj - base.open_adj ) / base.open_adj
            pnl.ret_open_low_entry = ( base.low_adj - base.open_adj ) / base.open_adj
            pnl.ret_open_high_exit = ( base.high_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)
            pnl.ret_open_low_exit = ( base.low_adj - base.close_adj.shift(1) ) / base.close_adj.shift(1)

            base.liquidity = base.volume.fillna( method='ffill', limit=10 ) * base.close.fillna( method='ffill', limit=10 )
            base.liquidity5 = base.liquidity.rolling( window=5, min_periods=0 ).mean()
            base.liquidity15 = base.liquidity.rolling( window=15, min_periods=0 ).mean()
            base.liquidity30 = base.liquidity.rolling( window=30, min_periods=0 ).mean()

            base.close_adj_ewma5  = base.close_adj.ewm( span=5 ).mean() / base.close_adj
            base.close_adj_ewma15 = base.close_adj.ewm( span=15 ).mean() / base.close_adj
            base.close_adj_ewma30 = base.close_adj.ewm( span=30 ).mean() / base.close_adj
            base.close_adj_ewma60 = base.close_adj.ewm( span=60 ).mean() / base.close_adj

            base.adv5 = base.volume.rolling( window=5, min_periods=0 ).mean()
            base.adv15 = base.volume.rolling( window=15, min_periods=0 ).mean()
            base.adv30 = base.volume.rolling( window=30, min_periods=0 ).mean()
            cutils.create_cache( base, n_days_refresh=n_days_refresh )
            cutils.create_cache( pnl, n_days_refresh=n_days_refresh )
        else:
            if load_type == "base" or verbose:
                solbasic.logger.info( "Loading base data and stock pnl from cache..." )
            if load_type == "base":
                cutils.load_cache( base )
            elif "base" in d_fields_used.keys():
                cutils.load_cache( base, d_fields_used[ "base" ] )
            cutils.load_cache( pnl )
            base.close = cutils.getattr_cache( base, "close" )
    
        if load_type == "base":
            solbasic.logger.info( "Base data, pnl loaded: {} days, {} instruments.".format( base.close.shape[0], base.close.shape[1] ) )
        else:
            if "base" in d_fields_used.keys():
                if verbose:
                    solbasic.logger.debug( "Base data loaded." )
            elif load_type == "pnl":
                solbasic.logger.info( "Pnl loaded: {} days, {} instruments.".format( pnl.ret.shape[0], pnl.ret.shape[1] ) )
            else:
                solbasic.logger.warn( "load_type={} unsupported.".format( load_type ) )


    def base_data_etf( self, d_fields_used={}, load_type="base", n_days_refresh=None, verbose=False ):
        pass


    def gics_groups( self, mode_neutral=None, n_days_refresh=None ):
        """ prepare gics group classification """
        mode_neutral = solbasic.sync_mod_attr( mode_neutral, cfg, "mode_neutral" )
        if cfg.b_recreate_cache or not cutils.exists_cache( gicsg ):
            solbasic.logger.info( "Building gics group classification..." )
            df_gicsg = self.exec_top( self.data_mgr.load_gics_groups, b_union=True )
            test.df_gicsg = df_gicsg.copy()
            if df_gicsg.empty:
                solbasic.logger.warn( "Gics groups skipped." )
                return
            solbasic.logger.debug( "Gics groups original dimensions: {} days, {} instruments.".format( df_gicsg.gind.shape[0], df_gicsg.gind.shape[1] ) )
            if cfg.sim_mode == "cache":
                gicsg.gsector = df_gicsg.gsector.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.gind = df_gicsg.gind.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.gsubind = df_gicsg.gsubind.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.naicsh = df_gicsg.naicsh.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.sich = df_gicsg.sich.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.spcindcd = df_gicsg.spcindcd.fillna( method='ffill' ).fillna( method='bfill' )
                gicsg.spcseccd = df_gicsg.spcseccd.fillna( method='ffill' ).fillna( method='bfill' )
            else:
                if mode_neutral == 1:
                    gicsg.gsector = df_gicsg.gsector.fillna( method='ffill' ).fillna( method='bfill' )
                elif mode_neutral == 2:
                    gicsg.gind = df_gicsg.gind.fillna( method='ffill' ).fillna( method='bfill' )
                elif mode_neutral == 3:
                    gicsg.gsubind = df_gicsg.gsubind.fillna( method='ffill' ).fillna( method='bfill' )
                else:
                    gicsg.gsector = df_gicsg.gsector.fillna( method='ffill' ).fillna( method='bfill' )
                    gicsg.gind = df_gicsg.gind.fillna( method='ffill' ).fillna( method='bfill' )
                    gicsg.gsubind = df_gicsg.gsubind.fillna( method='ffill' ).fillna( method='bfill' )
            cutils.create_cache( gicsg )
        else:
            solbasic.logger.info( "Loading gics group classification from cache..." )
        gicsg.gsector = cutils.getattr_cache( gicsg, "gsector" )
        gicsg.gind = cutils.getattr_cache( gicsg, "gind" )
        gicsg.gsubind = cutils.getattr_cache( gicsg, "gsubind" )
        gicsg.naicsh = cutils.getattr_cache( gicsg, "naicsh" )
        gicsg.sich = cutils.getattr_cache( gicsg, "sich" )
        gicsg.spcindcd = cutils.getattr_cache( gicsg, "spcindcd" )
        gicsg.spcseccd = cutils.getattr_cache( gicsg, "spcseccd" )
        solbasic.logger.info( "Gics groups loaded: {} days, {} instruments.".format( gicsg.gsubind.shape[0], gicsg.gsubind.shape[1] ) )


    def rv_groups( self ):
        pass


    def compustat( self, d_fields_used={}, n_days_refresh=None, verbose=False ):
        """ prepare Compustat data """
        tp = [ None ] * 5
        if cfg.b_recreate_cache or cfg.b_refresh_cache or not cutils.exists_cache( cs ) or not cutils.exists_cache( csf ) or not cutils.exists_cache( csn ) or not cutils.exists_cache( csq ) or not cutils.exists_cache( csa ):
            if cfg.b_recreate_cache: 
                solbasic.logger.info( "Building Compustat..." )
            elif cfg.b_refresh_cache and n_days_refresh != None:                                      
                solbasic.logger.info( "Refreshing last {} days of Compustat...".format( n_days_refresh ) )   
            else:
                solbasic.logger.info( "Building Compustat cache..." )

            df_cs = self.data_mgr.load_compustat( self.conn, n_days_refresh=n_days_refresh )
            test.df_cs = df_cs.copy()
            d_df_cs = {}
            for item in glb.cs_fields:
                df_tmp = df_cs.query( "item == \'{}\'".format( item.upper() ) ).sort_values( by="tdate" )
                d_df_cs[ item ] = self.exec_top( df_tmp, mode="raw", b_union=True ).value
            # popolate and match index of base data
            if df_cs.empty:
                solbasic.logger.warn( "Compustat skipped." )
                return tp
            
            # raw cs matrices as-is, ts index being the union with daily close
            cutils.add_mod_dict( d_df_cs, cs )
            # adjust per share stats by splits and div.
            l_fields_adj = [ "aqdq", "aqepsq", "arceepsq", "arcedq", "epsf12", "epsfiq", "epsfxq", "epspiq", "epspxq", "epsx12", "dteepsq", "dtepq", "gdwlid12", "gdwlidq", "gdwlieps12", "gdwliepsq","glced12", "glcedq", "glced12", "glcedq", "glceeps12", "glceepsq", "gldq", "glepsq", "nrtxtdq", "nrtxtepsq", "oepf12", "oeps12", "oepsxq", "opepsq", "pncd12", "pncdq", "pnceps12", "pncepsq", "pncidpq", "pncidq", "pnciepspq", "pnciepsq", "pncpd12", "pncpdq", "pncpeps12", "pncpepsq", "pncwidpq", "pncwidq", "pncwiepq", "pncwiepsq", "prcd12", "prcdq", "prceps12", "prcepsq", "prcpd12", "prcpdq", "prcpeps12", "prcpepsq", "prcraq", "rcdq", "rcepsq", "rdipdq", "rdipepsq", "rrd12", "rrdq", "rreps12", "rrepsq" ,"setd12", "setdq", "seteps12", "setepsq", "spcedpq", "spcedq", "spceeps12", "spceepsp12", "spceepspq", "spceepsq", "spcepd12", "spidq", "spiepsq", "wddq", "wdepsq","xoptd12", "xoptd12p", "xoptdq", "xoptdqp", "xopteps12", "xoptepsp12", "xoptepsq", "xoptepsqp" ]
            l_fields_qty = [ "csh12q", "cshfd12", "cshfdq", "cshiq", "cshopq", "cshoq", "cshprq", "pnrshoq", "prshoq", "tstknq" ]
	    cutils.adjust_mod( l_fields_adj, cs, mode='*' )
	    cutils.adjust_mod( l_fields_qty, cs, mode='/' )
            # list to normalize by cap
            l_fields_norm = list( set( glb.cs_fields ) - set( [ "cusip", "tdate" ] ) - set( l_fields_adj ) - set( l_fields_qty ) )
            # front fill
            cutils.ffill_mod( cs, csf )
            # cap normalized fields
	    cutils.normalize_mod( l_fields_norm, csf, csn )
            # percent change per quarter
            cutils.set_mod_pct_chg( cs, csq )
            # percent change from same quarter last year
            cutils.set_mod_pct_chg_annual( csf, csa )
            cutils.create_cache( cs, n_days_refresh=n_days_refresh )
            cutils.create_cache( csf, n_days_refresh=n_days_refresh )
            cutils.create_cache( csn, n_days_refresh=n_days_refresh )
            cutils.create_cache( csq, n_days_refresh=n_days_refresh )
            cutils.create_cache( csa, n_days_refresh=n_days_refresh )
        else:
            if cfg.b_prepare_data or verbose:
                solbasic.logger.info( "Loading Compustat data from cache..." )
            if cfg.b_prepare_data:
                #cutils.load_cache( cs )
                #cutils.load_cache( csf )
                #cutils.load_cache( csn )
                #cutils.load_cache( csq )
                #cutils.load_cache( csa )
                ##memory efficient
                csit = cutils.load_cache_interactive( cs )
                csfit = cutils.load_cache_interactive( csf )
                csnit = cutils.load_cache_interactive( csn )
                csqit = cutils.load_cache_interactive( csq )
                csait = cutils.load_cache_interactive( csa )
                tp = [ csit, csfit, csnit, csqit, csait ]
                solbasic.logger.info( "Compustat data loaded as store: {} days, {} instruments.".format( csit.acoq.shape[0], csit.acoq.shape[1] ) )
                return tp
            else:
                if "cs" in d_fields_used.keys():
                    cutils.load_cache( cs, d_fields_used[ "cs" ] )
                if "csf" in d_fields_used.keys():
                    cutils.load_cache( csf, d_fields_used[ "csf" ] )
                if "csn" in d_fields_used.keys():
                    if verbose:
                        solbasic.logger.debug( "Loading {} from csn ...".format( d_fields_used[ "csn" ] ) )
                    cutils.load_cache( csn, d_fields_used[ "csn" ] )
                if "csq" in d_fields_used.keys():
                    if verbose:
                        solbasic.logger.debug( "Loading {} from csq ...".format( d_fields_used[ "csq" ] ) )
                    cutils.load_cache( csq, d_fields_used[ "csq" ] )
                if "csa" in d_fields_used.keys():
                    if verbose:
                        solbasic.logger.debug( "Loading {} from csa ...".format( d_fields_used[ "csa" ] ) )
                    cutils.load_cache( csa, d_fields_used[ "csa" ] )

        if cfg.b_prepare_data:
            solbasic.logger.info( "Compustat data loaded: {} days, {} instruments.".format( cs.acoq.shape[0], cs.acoq.shape[1] ) )
        elif verbose:
            solbasic.logger.debug( "Compustat data loaded." )
        return tp



    def capital_iq( self, d_fields_used={}, n_days_refresh=None, verbose=False ):
        pass


    def worldscope( self, d_fields_used={}, n_days_refresh=None, verbose=False ):
        pass


    def reuters_fundamental( self, d_fields_used={}, n_days_refresh=None, verbose=False ):
        pass


    def factset_fundamental( self, d_fields_used={}, n_days_refresh=None, verbose=False ):
        pass
