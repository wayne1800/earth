
""" alpha class """

import re
import copy
import math
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel
from sol.core import basic as solbasic
from sol import calcperf
from earth.conf import cfg
from earth.conf import glb
from earth.cache import dmgr
from earth.mod import univ
from earth.mod import base
from earth.pnl import pnl
from earth.mod import adj
from earth.mod import cs
from earth.mod import csf
from earth.mod import csn
from earth.mod import csq
from earth.mod import csa
from earth.mod import gicsg
from earth.mod import rvg
from earth.mod import test
from earth.alpha import ops
from earth.alpha import autils

class alpha:
    """ class to accomodate alphas, stats and operations """

    def __init__( self, expr='', name='', d_configs=None, b_ops=True, b_stat=True, mat_raw=DataFrame() ):
        """ 
        expr: alpha expression
        name: alpha name
        d_configs: preset alpha configs
        b_ops: run a series of operations on alphas
        b_stat: calculate performance metrics
        mat_raw: define alpha with matrix instead of expr. 
        """
        d_configs = solbasic.sync_mod_attr( d_configs, cfg, "d_configs" )
        mat = DataFrame()
        self.expr = expr
        self.name = name
        self.expr_raw = expr
        self.d_configs = {}
        # simulation properties, mutable
        self.d_configs[ "start_date" ]       = solbasic.get_dict( d_configs, "start_date", cfg.start_date )
        self.d_configs[ "end_date" ]         = solbasic.get_dict( d_configs, "end_date", cfg.end_date )
        self.d_configs[ "sim_days_start" ]   = solbasic.get_dict( d_configs, "sim_days_start", cfg.sim_days_start )
        self.d_configs[ "sim_days_is_vs" ]   = solbasic.get_dict( d_configs, "sim_days_is_vs", cfg.sim_days_is_vs )
        self.d_configs[ "sim_days_end" ]     = solbasic.get_dict( d_configs, "sim_days_end", cfg.sim_days_end )
        self.d_configs[ "load_backdays" ]    = solbasic.get_dict( d_configs, "load_backdays", cfg.load_backdays )
        self.d_configs[ "mode_pnl" ]         = solbasic.get_dict( d_configs, "mode_pnl", cfg.mode_pnl )
        self.d_configs[ "mode_tcost" ]       = solbasic.get_dict( d_configs, "mode_tcost", cfg.mode_tcost )
        self.d_configs[ "book_size" ]        = solbasic.get_dict( d_configs, "book_size", cfg.book_size )
        # innate properties, immutable. Affects performance
        self.d_configs[ "dataset" ]          = solbasic.get_dict( d_configs, "dataset", cfg.dataset )
        self.d_configs[ "mode_neutral" ]     = solbasic.get_dict( d_configs, "mode_neutral", cfg.mode_neutral )
        self.d_configs[ "delay" ]            = solbasic.get_dict( d_configs, "delay", cfg.delay )
        self.d_configs[ "n_in_play" ]        = solbasic.get_dict( d_configs, "n_in_play", cfg.n_in_play )
        self.d_configs[ "f_wins_pctl" ]      = solbasic.get_dict( d_configs, "f_wins_pctl", cfg.f_wins_pctl )
        self.d_configs[ "f_trun_frac" ]      = solbasic.get_dict( d_configs, "f_trun_frac", cfg.f_trun_frac )
        self.d_configs[ "n_days_decay" ]     = solbasic.get_dict( d_configs, "n_days_decay", cfg.n_days_decay )
        self.d_configs[ "n_days_corr_pt" ]   = solbasic.get_dict( d_configs, "n_days_corr_pt", cfg.n_days_corr_pt )
        self.d_configs[ "n_days_corr_pnl" ]  = solbasic.get_dict( d_configs, "n_days_corr_pnl", cfg.n_days_corr_pnl )
        self.b_ops = b_ops
        self.b_stat = b_stat
        if self.expr:
            self.d_configs[ "d_fields_used" ] = autils.get_fields_used( self.expr )
            try:
                self.mat_raw = eval( self.expr )
            except AttributeError:
                solbasic.logger.warn( "Unable to evaluate {}, set mat_raw=1.".format( self.expr ) )
                self.mat_raw = 1
        elif not mat_raw.empty:
            self.mat_raw = mat_raw
        if hasattr( self, "mat_raw" ):
            self.init_mat_params()
            self.mat_raw *= univ.mat_in_play # set valid inst again to make sure. Not enough to truncate in dmgr.

        # apply ops and get stats
        if hasattr( self, "mat_raw" ) and not self.mat_raw.empty:
            if b_ops:
                self.ops()
            if b_stat:
                self.stats()

        # corner checks
        if self.d_configs[ "sim_days_start" ] != 0 and self.d_configs[ "sim_days_start" ] < self.d_configs[ "sim_days_end" ]:
            if not hasattr( glb, "b_warn_end_prior_start" ):
                solbasic.logger.warn( "pid: {}, alpha end_date {} prior to start_date {}. Force end at data end.".format( os.getpid(), self.d_configs[ "sim_days_start" ], self.d_configs[ "sim_days_end" ] ) )
                if cfg.b_alphagen_on:
                    glb.b_warn_end_prior_start = True
            self.d_configs[ "sim_days_end" ] = 1
            

    def init_mat_params( self ):
        # truncate to US trading days
        self.mat_raw = self.mat_raw.ix[ univ.mat_in_play.index ]
	self.d_configs[ "start_date_alpha" ], self.d_configs[ "end_date_alpha" ], self.d_configs[ "is_vs_date_alpha" ] = autils.get_dates_alpha( self )

        # check if room for backdays
        if self.mat_raw.shape[0] < self.d_configs[ "load_backdays" ] + self.d_configs[ "sim_days_start" ] - self.d_configs[ "sim_days_end" ] + 1:
            if self.mat_raw.shape[0] < self.d_configs[ "sim_days_start" ] - self.d_configs[ "sim_days_end" ] + 1:
                self.d_configs[ "sim_days_start" ] = 0
                if not hasattr( glb, "b_warn_tight_data" ):
                    solbasic.logger.warn( "Alpha series covers {} days, not long enough to cover sim period ({}). Force start from beginning.".format( self.mat_raw.shape[0], self.d_configs[ "sim_days_start" ] - self.d_configs[ "sim_days_end" ] + 1 ) )
                    if cfg.b_alphagen_on:
                        glb.b_warn_tight_data = True
            elif not hasattr( glb, "b_warn_tight_data_bday" ):
                if not cfg.b_alpha_combo:
                    solbasic.logger.warn( "Alpha series covers {} days, between sim period ({}) and sim period + backdays ({}). Backdays compromised.".format( self.mat_raw.shape[0], self.d_configs[ "sim_days_start" ], self.d_configs[ "load_backdays" ] + self.d_configs[ "sim_days_start" ] - self.d_configs[ "sim_days_end" ] + 1 ) )
                if cfg.b_alphagen_on:
                    glb.b_warn_tight_data_bday = True


    def __str__( self ):
        return self.expr


    def __repr__( self ):
        if hasattr( self, "summary" ):
            return "b/c: {}; a/c: {}".format( self.summary, self.summary100 )
        else:
            return ''


    def __len__( self ):
        return self.mat_raw.shape[0]


    def head( self, n ):
        """ get the first n elements """
        return self.mat_raw.ix[:n]


    def tail( self, n ):
        """ get the last n elements """
        return self.mat_raw.ix[-n:]


    @classmethod
    def from_string( cls, string ):
        return cls( string, b_ops=False, b_stat=False )


    def neutralize( self, mode=None ):
        """ neutralize the positions. 0: by market, 1: by sector, 2: by group, 3: by subgroup. See comments for more modes """
        if mode == None:
            mode = self.d_configs[ "mode_neutral" ]
        self.expr = "ops.neutralize( {}, {} )".format( self.expr, mode )
        if hasattr( self, "mat" ):
            if "combo" in self.expr_raw and not self.b_alpha_combo:
                self.mat *= univ.mat_in_play
            self.mat = ops.neutralize( self.mat, mode=mode )
        else:
            if "combo" in self.expr_raw and not self.b_alpha_combo:
                self.mat_raw *= univ.mat_in_play
            self.mat = ops.neutralize( self.mat_raw, mode=mode )
        return self


    def normalize( self, book_size=None ):
        """ normalize to book size """
        self.expr = "ops.normalize( {}, {} )".format( self.expr, book_size )
        if hasattr( self, "mat" ):
            self.mat = ops.normalize( self.mat, book_size=book_size )
        else:
            self.mat = ops.normalize( self.mat_raw, book_size=book_size )
        return self
    
    
    def truncate( self, frac=None ):
        """ truncate the max position to be frac of total abs position. Caveat: won't work for alphas designed for # instruments < 1/frac """
        if frac < 0:
            solbasic.logger.warn( "frac={} invalid, set default {}.".format( frac, cfg.f_trun_frac ) )
            frac = cfg.f_trun_frac
        self.expr = "ops.truncate( {}, {} )".format( self.expr, frac )
        if hasattr( self, "mat" ):
            self.mat = ops.truncate( self.mat, frac=frac )
        else:
            self.mat = ops.truncate( self.mat_raw, frac=frac )
        return self


    def truncate_tvr( self, frac=None ):
        """ truncate the max turnover per stock lower / higher than pctl """
        if frac < 0:
            solbasic.logger.warn( "frac={} invalid, set default {}.".format( frac, cfg.f_trun_frac ) )
            frac = cfg.f_trun_frac
        self.expr = "ops.truncate_tvr( {}, {} )".format( self.expr, frac )
        if hasattr( self, "mat" ):
            self.mat = ops.truncate_tvr( self.mat, frac=frac )
        else:
            self.mat = ops.truncate_tvr( self.mat_raw, frac=frac )
        return self


    def winsorize( self, q=None ):
        """ set extreme data points at two ends to q and 1-q percentile """
        self.expr = "ops.winsorize( {}, {} )".format( self.expr, q )
        if hasattr( self, "mat" ):
            self.mat = ops.winsorize( self.mat, q=q )
        else:
            self.mat = ops.winsorize( self.mat_raw, q=q )
        return self


    def winsorize_tvr( self, q=None ):
        """ truncate the max turnover per stock lower / higher than pctl """
        self.expr = "ops.winsorize_tvr( {}, {} )".format( self.expr, q )
        if hasattr( self, "mat" ):
            self.mat = ops.winsorize_tvr( self.mat, q=q )
        else:
            self.mat = ops.winsorize_tvr( self.mat_raw, q=q )
        return self


    def decay( self, n=None, mode="exponential" ):
        """ decay alpha positions by n days. 
            mode: unweighted, exponential, linear """
        self.expr = "ops.decay( {}, {} )".format( self.expr, n )
        if hasattr( self, "mat" ):
            self.mat = ops.decay( self.mat, n=n, mode=mode )
        else:
            self.mat = ops.decay( self.mat_raw, n=n, mode=mode )
        return self


    def __compute_pnl( self, mode_pnl=None, mode_tcost=None, l_inst=None, verbose=True ):
        """ mode_pnl: 0: close pnl. 1: MOOMOC pnl.
            mode_tcost: 0: pure signal calculation only, 1: full tcost tiers calculation in pnl. 
            Note mat_alpha is delayed twice for d1, once for d0 """
        mode_pnl = solbasic.sync_mod_attr( mode_pnl, cfg, "mode_pnl" )
        mode_tcost = solbasic.sync_mod_attr( mode_tcost, cfg, "mode_tcost" )
        mat_alpha = self.mat.copy().shift(1).fillna(0)
        if self.d_configs[ "book_size" ] > 1:
            mat_alpha = np.round( mat_alpha * self.d_configs[ "book_size" ] / base.close.shift(2) ) * base.close.shift(2)
        mat_alpha.dropna( axis=1, how='all', inplace=True )
        mat_tvr = np.abs( mat_alpha - mat_alpha.shift(1) )
        cum_tvr = mat_tvr.sum().sum()
        if mode_pnl == 0:
            mat_pnl = mat_alpha.shift( self.d_configs[ "delay" ] ) * pnl.ret.ix[ mat_alpha.index, mat_alpha.columns ] * cfg.leverage
        elif mode_pnl == 1:
            mat_pnl = mat_alpha.shift( self.d_configs[ "delay" ] ) * pnl.ret_open_entry.ix[ mat_alpha.index, mat_alpha.columns ] * cfg.leverage
        else:
            solbasic.logger.warn( "Mode not supported." )

        test.mat_raw = self.mat_raw.copy()
        test.mat_alpha = mat_alpha.copy()
        test.mat_pnl = mat_pnl.copy()
        self.tvr = ( 1 + cum_tvr / self.d_configs[ "book_size" ] ) / mat_alpha.shape[0] * 100
        mat_pnl = mat_pnl.replace( [ np.inf, -np.inf ], np.nan )
        self.v_pnl = mat_pnl.sum( axis=1 )
        self.v_cum_pnl = self.v_pnl.cumsum()
        self.mat_pnl = mat_pnl

        if self.d_configs[ "mode_tcost" ]:
            if self.d_configs[ "delay" ] >= 0:
                if self.d_configs[ "book_size" ] != 1:
                    mat_shares_traded = np.round( ( mat_tvr / base.close ) )
                    total_shares_traded = mat_shares_traded.sum().sum()
                    mat_pnl100 = mat_pnl - mat_tvr * pnl.tcost50 - mat_shares_traded * cfg.commission - mat_tvr / 2 * 0.00002
                else:
                    mat_pnl100 = mat_pnl - mat_tvr * pnl.tcost50 - mat_tvr / 2 * 0.00002

            else:
                solbasic.logger.warn( "Delay not supported. Use 0 or positive integer." )

                mat_pnl100 = mat_pnl100.replace( [ np.inf, -np.inf ], np.nan )
            mat_pnl100 = DataFrame( mat_pnl100, index=mat_pnl.index, columns=mat_pnl.columns )
            self.v_pnl100 = mat_pnl100.sum( axis=1 )

            self.v_cum_pnl100 = self.v_pnl100.cumsum()
            self.mat_pnl100 = mat_pnl100
            v_tcost = self.v_pnl - self.v_pnl100
            self.v_pnl25 = self.v_pnl - v_tcost * 0.25 
            self.v_pnl50 = self.v_pnl - v_tcost * 0.50
            self.v_cum_pnl25 = self.v_pnl25.cumsum()
            self.v_cum_pnl50 = self.v_pnl50.cumsum()
        #self.mat_tcost = mat_pnl - mat_pnl100
        self._set_alpha_stats( mode_tcost=self.d_configs[ "mode_tcost" ], l_inst=l_inst, verbose=verbose )
        return self
    

    def ops( self ):
        """ alpha operations to go through """
        if not self.expr_raw:
            self.expr_raw = self.expr
        if not hasattr( self, "mat" ):
            self.mat = self.mat_raw.copy()
        if "combo" in self.expr_raw:
            return self.winsorize( self.d_configs[ "f_wins_pctl" ] ).truncate( self.d_configs[ "f_trun_frac" ] ).normalize( self.d_configs[ "book_size" ] )
        else:
            return self.winsorize( self.d_configs[ "f_wins_pctl" ] ).neutralize( self.d_configs[ "mode_neutral" ] ).truncate( self.d_configs[ "f_trun_frac" ] ).neutralize( self.d_configs[ "mode_neutral" ] ).normalize( self.d_configs[ "book_size" ] )


    def simplify( self, mode="deep" ):
        """ 
            remove all dfs, keep two years of pnl and one month of position, and prepare to save to storage. Simplified alphas will circulate in shared namespace of multi-processes for corr skim. 
            mode="deep": truncate as such; "shallow": keep full v_pnl*, v_cum_pnl* and mat_raw and remove the rest.
        """
        l_attr_to_rm = [ attr for attr in self.__dict__.keys() if "mat" in attr or "v_" in attr ]
        for attr in l_attr_to_rm:
            if attr == "mat_raw" or attr == "mat" or "mat_exposure" in attr:
                if mode == "deep":
                    setattr( self, attr, getattr( self, attr ).ix[ -self.d_configs[ "n_days_corr_pt" ]: ] )
                elif mode == "shallow":
                    pass
                else:
                    solbasic.logger.warn( "Alpha simplification mode={} not supported, taken as shallow.".format( mode ) )
            elif "v_pnl" in attr:
                if mode == "deep":
                    setattr( self, attr, getattr( self, attr )[ -self.d_configs[ "n_days_corr_pnl" ]: ] )
                elif mode == "shallow":
                    pass
                else:
                    solbasic.logger.warn( "Alpha simplification mode={} not supported, taken as shallow.".format( mode ) )
            elif "v_cum_pnl" in attr and mode == "shallow":
                pass
            else:
                solbasic.remove_attr( self, attr )
        return self


    def _set_alpha_stats( self, mode_tcost=None, l_inst=None, verbose=True ):
        """ truncate and calculate relevant alpha statistics, given v_pnl, mat_pnl in self 
            """
        mode_tcost = solbasic.sync_mod_attr( mode_tcost, cfg, "mode_tcost" )
        if l_inst and type( l_inst ) is list:
            l_inst = self.mat_raw.columns.intersection( l_inst )
            mat_alpha = self.mat.copy().shift(1)[ l_inst ].fillna(0)
            if mat_alpha.dropna( how='all' ).dropna( how='all', axis=1 ).empty:
                return
            self.v_pnl = self.mat_pnl.ix[ self.v_pnl.index, l_inst ].sum( axis=1 )
            if self.v_pnl[ self.v_pnl != 0 ].dropna().empty:
                return
            self.v_cum_pnl = self.v_pnl.cumsum()
            solbasic.logger.debug( "Computing stats on {} ...".format( l_inst ) )
        else:
            mat_alpha = self.mat.copy().shift(1).fillna(0)

        mean_pnl = self.v_pnl.mean() / self.d_configs[ "book_size" ] * cfg.n_bdays
        self.volatility = self.v_pnl.std() / self.d_configs[ "book_size" ] * np.sqrt( cfg.n_bdays )
        self.ir = mean_pnl / self.volatility
        self.volatility *= 100
        self.ret = mean_pnl * 100
        self.mdd, self.mdd_length, self.mdd_dates, self.mdd_sdate, self.mdd_edate = autils.max_dd( self.v_cum_pnl )
        self.mdd /= self.d_configs[ "book_size" ] / 100.
        self.avg_pos_l = mat_alpha[ mat_alpha > 0 ].ix[1:].sum( axis=1 ).mean()
        self.avg_pos_s = mat_alpha[ mat_alpha < 0 ].ix[1:].sum( axis=1 ).mean()
        self.summary = "{:.2f} {:.2f} {:.2f} {:.2f}% {:.2f}% {:.2f}% {:.2f}% {} {}".format( self.avg_pos_l, self.avg_pos_s, self.ir, self.ret, self.volatility, self.tvr, self.mdd, self.mdd_length, self.mdd_dates )

        if mode_tcost:
            if not hasattr( self, "v_pnl25" ):
                solbasic.logger.warn( "Requested to calculate alpha stats tiers, but v_pnl25 absent in alpha. Skip." )
                return
            mean_pnl25 = self.v_pnl25.mean() / self.d_configs[ "book_size" ] * cfg.n_bdays
            self.volatility25 = self.v_pnl25.std() / self.d_configs[ "book_size" ] * np.sqrt( cfg.n_bdays )
            self.ir25 = mean_pnl25 / self.volatility25
            self.volatility25 *= 100
            self.ret25 = mean_pnl25 * 100
            self.mdd25, self.mdd25_length, self.mdd25_dates, self.mdd25_sdate, self.mdd25_edate = autils.max_dd( self.v_cum_pnl25 )
            self.mdd25 /= self.d_configs[ "book_size" ] / 100.

            mean_pnl50 = self.v_pnl50.mean() / self.d_configs[ "book_size" ] * cfg.n_bdays
            self.volatility50 = self.v_pnl50.std() / self.d_configs[ "book_size" ] * np.sqrt( cfg.n_bdays )
            self.ir50 = mean_pnl50 / self.volatility50
            self.volatility50 *= 100
            self.ret50 = mean_pnl50 * 100
            self.mdd50, self.mdd50_length, self.mdd50_dates, self.mdd50_sdate, self.mdd50_edate = autils.max_dd( self.v_cum_pnl50 )
            self.mdd50 /= self.d_configs[ "book_size" ] / 100.

            mean_pnl100 = self.v_pnl100.mean() / self.d_configs[ "book_size" ] * cfg.n_bdays
            self.volatility100 = self.v_pnl100.std() / self.d_configs[ "book_size" ] * np.sqrt( cfg.n_bdays )
            self.ir100 = mean_pnl100 / self.volatility100
            self.volatility100 *= 100
            self.ret100 = mean_pnl100 * 100
            self.mdd100, self.mdd100_length, self.mdd100_dates, self.mdd100_sdate, self.mdd100_edate = autils.max_dd( self.v_cum_pnl100 )
            self.mdd100 /= self.d_configs[ "book_size" ] / 100.

            self.summary25 = "{:.2f} {:.2f} {:.2f} {:.2f}% {:.2f}% {:.2f}% {:.2f}% {} {}".format( self.avg_pos_l, self.avg_pos_s, self.ir25, self.ret25, self.volatility25, self.tvr, self.mdd25, self.mdd25_length, self.mdd25_dates )
            self.summary50 = "{:.2f} {:.2f} {:.2f} {:.2f}% {:.2f}% {:.2f}% {:.2f}% {} {}".format( self.avg_pos_l, self.avg_pos_s, self.ir50, self.ret50, self.volatility50, self.tvr, self.mdd50, self.mdd50_length, self.mdd50_dates )
            self.summary100 = "{:.2f} {:.2f} {:.2f} {:.2f}% {:.2f}% {:.2f}% {:.2f}% {} {}".format( self.avg_pos_l, self.avg_pos_s, self.ir100, self.ret100, self.volatility100, self.tvr, self.mdd100, self.mdd100_length, self.mdd100_dates )


    def reverse( self ):
        """ reverse alpha sign and compute statistics """
        if not ( "v_pnl" in dir( self ) and "mat_pnl" in dir( self ) ) or self.v_pnl.empty or self.mat_pnl.empty:
            self.__compute_pnl( mode_pnl=self.d_configs[ "mode_pnl" ], mode_tcost=self.d_configs[ "mode_tcost" ] )
        self.expr = '-(' + self.expr + ')'
        self.expr_raw = '-(' + self.expr_raw + ')'
        self.mat_raw = -self.mat_raw
        self.mat = -self.mat
        self.v_pnl = -self.v_pnl
        self.v_cum_pnl = -self.v_cum_pnl
        if self.d_configs[ "mode_tcost" ] and hasattr( self, "mat_pnl100" ):
            mat_tcost = self.mat_pnl - self.mat_pnl100
            v_tcost = mat_tcost.sum( axis=1 )
            self.v_pnl25 = -self.v_pnl - v_tcost * 0.25
            self.v_cum_pnl25 = self.v_pnl25.cumsum()
            self.v_pnl50 = -self.v_pnl - v_tcost * 0.5
            self.v_cum_pnl50 = self.v_cum_pnl50.cumsum()
            self.v_pnl100 = -self.v_pnl - v_tcost
            self.v_cum_pnl100 = self.v_cum_pnl100.cumsum()
            self.mat_pnl = -self.mat_pnl
            self.mat_pnl100 = -self.mat_pnl100 - 2 * mat_tcost
        self._set_alpha_stats( mode_tcost=self.d_configs[ "mode_tcost" ] )
        return self


    def stats( self, mode_pnl=None, mode_tcost=None, start_date_alpha=None, end_date_alpha=None, l_inst=None, verbose=True ):
        """ Compute statistics for cumulative pnl vector. """
        mat2 = self.mat.copy()
        if mode_pnl is None:
            mode_pnl = self.d_configs[ "mode_pnl" ]
        if mode_tcost is None:
            mode_tcost = self.d_configs[ "mode_tcost" ]
        if start_date_alpha:
            sim_days_start = self.mat_raw.ix[ self.d_configs[ "start_date_alpha" ]: ].shape[0]
        else:
            sim_days_start = self.d_configs[ "sim_days_start" ]
        if end_date_alpha:
            sim_days_end = self.mat_raw.ix[ self.d_configs[ "end_date_alpha" ]: ].shape[0]
        else:
            sim_days_end = self.d_configs[ "sim_days_end" ]
        if sim_days_end == 1:
            self.mat = self.mat.ix[ -sim_days_start: ]
        else:
            self.mat = self.mat.ix[ -sim_days_start : -sim_days_end ]
        self.mat = self.mat.ix[ -sim_days_start : -sim_days_end ]
        test.mat = self.mat.copy()
        self.__compute_pnl( mode_pnl=mode_pnl, mode_tcost=mode_tcost, l_inst=l_inst, verbose=verbose )
        self.mat = mat2
        return self
