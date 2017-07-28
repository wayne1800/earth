
""" data managers """

import os
import gc
import copy
import logging
import datetime
from collections import Counter
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
from sol.cnx import sql
from sol.core import basic as solbasic
from sol.core import date as soldate
from earth.conf import cfg
from earth.conf import glb
from earth.cache import cutils
from earth.mod import univ
from earth.mod import test

class dmgr:
    """ load data from DB schemas """

    def load_univ( self, conn, n_days_refresh=None ):
        """ load universe from price volume data """
        if n_days_refresh == None:
            start_date = cfg.start_date
        else:
            start_date = soldate.date_add( -n_days_refresh, cfg.end_date )
        query = "SELECT b.{0}, a.datadate, a.prccd, a.cshtrd, b.isin, b.cusip, b.sedol, b.tic FROM XFLTarget.dbo.sec_dprc a INNER JOIN XFLTarget.dbo.cssecurity b ON a.gvkey = b.gvkey AND a.iid = b.iid AND a.datadate BETWEEN b.effdate AND b.thrudate WHERE a.curcdd = 'USD' AND b.cusip IS NOT NULL AND a.datadate >= '{1}' and a.datadate <= '{2}' ORDER BY b.cusip, a.datadate".format( cfg.iden_stock, start_date, cfg.end_date )
        solbasic.logger.info( "Building universe..." )
        l_fields = [ "gvkey", "tdate", "close", "volume", "isin", "cusip", "sedol", "ticker" ]
        df = pd.io.sql.read_sql( query, conn )
        df.columns = l_fields
        df[ "tdate" ] = pd.to_datetime( df[ "tdate" ] )
        solbasic.logger.info( "Query done." )
        return df
    

    def load_adj( self, conn ):
        """ load price volume data """
        if hasattr( glb, "str_in_play" ) and glb.str_in_play:
	    query = "SELECT b.{0}, a.datadate, a.ajexdi, c.adjex FROM XFLTarget.dbo.sec_dprc a INNER JOIN XFLTarget.dbo.cssecurity b ON a.gvkey = b.gvkey AND a.iid = b.iid AND a.datadate BETWEEN b.effdate AND b.thrudate INNER JOIN XFLTarget.dbo.co_adjfact c ON a.gvkey = c.gvkey WHERE a.curcdd = 'USD' AND b.{0} in ({1}) AND a.datadate BETWEEN c.effdate AND c.thrudate AND c.adjex IS NOT NULL AND a.datadate >= '{2}' and a.datadate <= '{3}' ORDER BY b.cusip, a.datadate".format( cfg.iden_stock, glb.str_in_play, cfg.start_date, cfg.end_date )
        else:
	    query = "SELECT b.{0}, a.datadate, a.ajexdi, c.adjex FROM XFLTarget.dbo.sec_dprc a INNER JOIN XFLTarget.dbo.cssecurity b ON a.gvkey = b.gvkey AND a.iid = b.iid AND a.datadate BETWEEN b.effdate AND b.thrudate INNER JOIN XFLTarget.dbo.co_adjfact c ON a.gvkey = c.gvkey WHERE a.curcdd = 'USD' AND a.datadate BETWEEN c.effdate AND c.thrudate AND c.adjex IS NOT NULL AND a.datadate >= '{1}' and a.datadate <= '{2}' ORDER BY b.cusip, a.datadate".format( cfg.iden_stock, cfg.start_date, cfg.end_date )

        solbasic.logger.info( "Querying adjusted factors..." )
        l_fields = [ cfg.iden_stock, "tdate", "adjdi", "adjcs" ]
        try:
            df = pd.io.sql.read_sql( query, conn )
            df.columns = l_fields
            df[ "tdate" ] = pd.to_datetime( df[ "tdate" ] )
        except ValueError: 
            solbasic.logger.warn( "Failed to load adjusted factors, no records retrieved." )
            df = DataFrame() 
        solbasic.logger.info( "Query done." )
        return df


    def load_prices( self, conn, n_days_refresh=None ):
        """ load price volume data """
        if n_days_refresh == None:
            start_date = cfg.start_date
        else:
            start_date = soldate.date_add( -n_days_refresh, cfg.end_date )
        if hasattr( glb, "str_in_play" ) and glb.str_in_play:
	    query = "SELECT b.{0}, a.datadate, a.prcod, a.prchd, a.prcld, a.prccd, a.cshtrd, a.cshoc FROM XFLTarget.dbo.sec_dprc a INNER JOIN XFLTarget.dbo.cssecurity b ON a.gvkey = b.gvkey AND a.iid = b.iid AND a.datadate BETWEEN b.effdate AND b.thrudate WHERE a.curcdd = 'USD' AND b.{0} in ({1}) AND a.datadate >= '{2}' and a.datadate <= '{3}' ORDER BY b.cusip, a.datadate".format( cfg.iden_stock, glb.str_in_play, start_date, cfg.end_date )
        else:
	    query = "SELECT b.{0}, a.datadate, a.prcod, a.prchd, a.prcld, a.prccd, a.cshtrd, a.cshoc FROM XFLTarget.dbo.sec_dprc a INNER JOIN XFLTarget.dbo.cssecurity b ON a.gvkey = b.gvkey AND a.iid = b.iid AND a.datadate BETWEEN b.effdate AND b.thrudate WHERE a.curcdd = 'USD' AND a.datadate >= '{1}' and a.datadate <= '{2}' ORDER BY b.cusip, a.datadate".format( cfg.iden_stock, start_date, cfg.end_date )

        solbasic.logger.info( "Querying price series..." )
        l_fields = [ cfg.iden_stock, "tdate", "open", "high", "low", "close", "volume", "sharesout" ]
        try:
            df = pd.io.sql.read_sql( query, conn )
            df.columns = l_fields
            df[ "tdate" ] = pd.to_datetime( df[ "tdate" ] )
        except ValueError: 
            solbasic.logger.warn( "Failed to load base data, no records retrieved." )
            df = DataFrame() 
        solbasic.logger.info( "Query done." )
        return df


    def load_gics_groups( self, conn, n_days_refresh=None ):
        """ load sector, industry and subindustry from Compustat """
        if n_days_refresh == None:
            start_date = cfg.start_date
        else:
            start_date = soldate.date_add( -n_days_refresh, cfg.end_date )
        str_fields = "{}, datadate, naicsh, sich, gind, gsector, gsubind, spcindcd, spcseccd".format( cfg.iden_stock )
        if glb.str_in_play:
            query = "SELECT s.{0} FROM XFLTarget.dbo.co_industry AS i INNER JOIN XFLTarget.dbo.security AS s ON i.gvkey = s.gvkey INNER JOIN XFLTarget.dbo.company c ON s.gvkey = c.gvkey WHERE s.{4} in ({1}) AND ( s.dldtei IS NULL OR s.dldtei >= '{2}' ) AND s.excntry = 'USA' AND i.datadate >= '{2}' AND i.datadate <= '{3}' ORDER BY s.cusip, i.datadate".format( str_fields, glb.str_in_play, start_date, cfg.end_date, cfg.iden_stock )
        else:
            query = "SELECT s.{0} FROM XFLTarget.dbo.co_industry AS i INNER JOIN XFLTarget.dbo.security AS s ON i.gvkey = s.gvkey INNER JOIN XFLTarget.dbo.company c ON s.gvkey = c.gvkey WHERE ( s.dldtei IS NULL OR s.dldtei >= '{2}' ) AND s.excntry = 'USA' AND i.datadate >= '{2}' AND i.datadate <= '{3}' ORDER BY s.cusip, i.datadate".format( str_fields, glb.str_in_play, start_date, cfg.end_date )
	l_fields = []
        l_fields.extend( str_fields.split( ', ' )[ len( l_fields ): ] )
        try:
            df = pd.io.sql.read_sql( query, conn )
            df = df.rename( columns = { "datadate" : "tdate" } )
            df[ "tdate" ] = pd.to_datetime( df[ "tdate" ] )
        except ValueError:
            solbasic.logger.warn( "Failed to load Compustat industry, no records retrieved." )
            df = DataFrame() 
        solbasic.logger.info( "Query done." )
        return df


    def load_compustat( self, conn, n_days_refresh=None ):
        """ load fields from Compustat """
        if n_days_refresh == None:
            start_date = cfg.start_date
        else:
            start_date = soldate.date_add( -n_days_refresh, cfg.end_date )
        str_fields = "acchgq, acomincq, acoq, actq, altoq, ancq, anoq, aociderglq, aociotherq, aocipenq, aocisecglq, aol2q, aoq, apq, aqaq, aqdq, aqepsq, aqpl1q, aqpq, arcedq, arceepsq, arceq, atq, aul3q, billexceq, capr1q, capr2q, capr3q, capsftq, capsq, ceiexbillq, ceqq, cheq, chq, cibegniq, cicurrq, ciderglq, cimiiq, ciotherq, cipenq, ciq, cisecglq, citotalq, cogsq, csh12q, cshfd12, cshfdq, cshiq, cshopq, cshoq, cshprq, cstkcvq, cstkeq, cstkq, dcomq, dd1q, deracq, derhedglq, derlcq, derlltq, diladq, dilavq, dlcq, dlttq, doq, dpacreq, dpactq, dpq, dpretq, drcq, drltq, dteaq, dtedq, dteepsq, dtepq, dvintfq, dvpq, epsf12, epsfi12, epsfiq, epsfxq, epspi12, epspiq, epspxq, epsx12, esopctq, esopnrq, esoprq, esoptq, esubq, fcaq, ffoq, finacoq, finaoq, finchq, findlcq, findltq, finivstq, finlcoq, finltoq, finnpq, finreccq, finrecltq, finrevq, finxintq, finxoprq, gdwlamq, gdwlia12, gdwliaq, gdwlid12, gdwlidq, gdwlieps12, gdwliepsq, gdwlipq, gdwlq, glaq, glcea12, glceaq, glced12, glcedq, glceeps12, glceepsq, glcepq, gldq, glepsq, glivq, glpq, hedgeglq, ibadj12, ibadjq, ibcomq, ibmiiq, ibq, icaptq, intaccq, intanoq, intanq, invfgq, invoq, invrmq, invtq, invwipq, ivaeqq, ivaoq, ivltq, ivstq, lcoq, lctq, lltq, lnoq, lol2q, loq, loxdrq, lqpl1q, lseq, ltmibq, ltq, lul3q, mibnq, mibq, mibtq, miiq, msaq, ncoq, niitq, nimq, niq, nopiq, npatq, npq, nrtxtdq, nrtxtepsq, nrtxtq, obkq, obq, oepf12, oeps12, oepsxq, oiadpq, oibdpq, opepsq, optdrq, optfvgrq, optlifeq, optrfrq, optvolq, piq, pllq, pnc12, pncd12, pncdq, pnceps12, pncepsq, pnciapq, pnciaq, pncidpq, pncidq, pnciepspq, pnciepsq, pncippq, pncipq, pncpd12, pncpdq, pncpeps12, pncpepsq, pncpq, pncq, pncwiapq, pncwiaq, pncwidpq, pncwidq, pncwiepq, pncwiepsq, pncwippq, pncwipq, pnrshoq, ppegtq, ppentq, prcaq, prcd12, prcdq, prce12, prceps12, prcepsq, prcpd12, prcpdq, prcpeps12, prcpepsq, prcpq, prcraq, prshoq, pstknq, pstkq, pstkrq, rcaq, rcdq, rcepsq, rcpq, rdipaq, rdipdq, rdipepsq, rdipq, recdq, rectaq, rectoq, rectq, rectrq, recubq, req, retq, reunaq, revtq, rllq, rra12, rraq, rrd12, rrdq, rreps12, rrepsq, rrpq, rstcheltq, rstcheq, saleq, seqoq, seqq, seta12, setaq, setd12, setdq, seteps12, setepsq, setpq, spce12, spced12, spcedpq, spcedq, spceeps12, spceepsp12, spceepspq, spceepsq, spcep12, spcepd12, spcepq, spceq, spidq, spiepsq, spioaq, spiopq, spiq, sretq, stkcoq, stkcpaq, teqq, tfvaq, tfvceq, tfvlq, tieq, tiiq, tstknq, tstkq, txdbaq, txdbcaq, txdbclq, txdbq, txdiq, txditcq, txpq, txtq, txwq, uacoq, uaoq, uaptq, ucapsq, ucconsq, uceqq, uddq, udmbq, udoltq, udpcoq, udvpq, ugiq, uinvq, ulcoq, uniamiq, unopincq, uopiq, updvpq, upmcstkq, upmpfq, upmpfsq, upmsubpq, upstkcq, upstkq, urectq, uspiq, usubdvpq, usubpcvq, utemq, wcapq, wdaq, wddq, wdepsq, wdpq, xaccq, xidoq, xintq, xiq, xoprq, xopt12, xoptd12, xoptd12p, xoptdq, xoptdqp, xopteps12, xoptepsp12, xoptepsq, xoptepsqp, xoptq, xoptqp, xrdq, xsgaq"
        if hasattr( glb, "str_in_play" ) and glb.str_in_play:
            query = "SELECT d.{4}, c.effdate, c.item, c.valuei FROM XFLTarget.dbo.csco_ikey a LEFT JOIN XFLTarget.dbo.csco_itxt b ON a.coifnd_id = b.coifnd_id JOIN ( SELECT coifnd_id, effdate, item, valuei FROM XFLTarget.dbo.csco_ifndq WHERE item IN ( {0} ) AND valuei IS NOT NULL ) c ON b.coifnd_id = c.coifnd_id AND c.effdate between b.effdate and b.thrudate JOIN XFLTarget.dbo.cssecurity d ON d.gvkey = a.gvkey WHERE b.item = 'UPDQ' AND b.effdate BETWEEN d.effdate AND d.thrudate AND d.{4} in ( {1} ) AND ( d.dldtei IS NULL OR d.dldtei >= '{2}' ) AND d.excntry = 'USA' AND c.effdate >= '{2}' and c.effdate <= '{3}' ORDER BY d.cusip, c.effdate, a.cyearq DESC, a.cqtr DESC".format( "\'{}\'".format( str_fields ).replace( ', ', "\', \'" ), glb.str_in_play, start_date, cfg.end_date, cfg.iden_stock )
        else: 
            query = "SELECT d.{3}, c.effdate, c.item, c.valuei FROM XFLTarget.dbo.csco_ikey a LEFT JOIN XFLTarget.dbo.csco_itxt b ON a.coifnd_id = b.coifnd_id JOIN ( SELECT coifnd_id, effdate, item, valuei FROM XFLTarget.dbo.csco_ifndq WHERE item IN ( {0} ) AND valuei IS NOT NULL ) c ON b.coifnd_id = c.coifnd_id AND c.effdate between b.effdate and b.thrudate JOIN XFLTarget.dbo.cssecurity d ON d.gvkey = a.gvkey WHERE b.item = 'UPDQ' AND b.effdate BETWEEN d.effdate AND d.thrudate AND ( d.dldtei IS NULL OR d.dldtei >= '{1}' ) AND d.excntry = 'USA' AND c.effdate >= '{1}' and c.effdate <= '{2}' ORDER BY d.cusip, c.effdate, a.cyearq DESC, a.cqtr DESC".format( "\'{}\'".format( str_fields ).replace( ',', "\', \'" ), start_date, cfg.end_date, cfg.iden_stock )
	l_fields = [ cfg.iden_stock, "tdate", "item", "value" ]
        l_fields_cs = str_fields.split( ', ' )
        glb.cs_fields = copy.deepcopy( l_fields_cs )
        try:
            test.query = query
            df = pd.io.sql.read_sql( query, conn )
            test.df_cs_raw = df.copy()
            df.columns = l_fields
            df[ "tdate" ] = pd.to_datetime( df[ "tdate" ] )
            df[ "tdate" ] = [ x - pd.Timedelta( hours=x.hour ) for x in df[ "tdate" ] ]
        except ValueError:
            solbasic.logger.warn( "Failed to load Compustat, no records retrieved." )
            df = DataFrame() 
        solbasic.logger.info( "Query done." )
        return df
