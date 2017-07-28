
""" alpha operations """

import traceback
import numpy as np
import pandas as pd
from pandas import DataFrame, Series, Panel, HDFStore
from sol.core import basic as solbasic
from earth.conf import cfg
from earth.conf import glb
from earth.mod import gicsg
from earth.mod import rvg


def addM( left, right ):
    return left + right

def subtractM( left, right ):
    return left - right

def multiplyM( left, right ):
    return left * right

def divideM( left, right ):
    try:
        return left / right
    except ZeroDivisionError:
        solbasic.logger.warn( "Divided by 0 and ill-defined. Set 0." )
        return 0

def signM( mat ):
    if type( mat ) is float:
        return mat
    return np.sign( mat ) * 100

def sign_power( mat, power=2, processed=False ):
    if type( mat ) is float:
        return mat
    if processed:
        norm = 34 # 3383 is mean of x**2 for x in [1, 100]
    else:
        norm = 1
    return np.sign( mat ) * ( np.abs( mat ) ** power ) / norm

def sign_sqrt( mat, power=0.5 ):
    if type( mat ) is float:
        return mat
    return np.sign( mat ) * ( np.abs( mat ) ** power )

def sign_log( mat ):
    """ leave elements<1 untouched """
    if type( mat ) is float:
        return mat
    try:
        mat_sign_log = np.sign( mat ) * np.log( np.abs( mat ) )
    except AttributeError:
        solbasic.logger.warn( "Unable to assign input mat of type {} to non-df output. Skip and return original matrix.".format( type( mat ) ) )
        return mat
    try:
        mat_sign_log[ np.abs( mat )<1 ] = mat
    except TypeError:
        solbasic.logger.warn( "Unable to assign input mat of type {} to non-df output type {}. Skip.".format( type( mat ), type( mat_sign_log ) ) )
    return mat_sign_log

def mean5M( mat, period=5 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=0 ).mean()

def mean30M( mat, period=30 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=0 ).mean()

def corr5M( mat1, mat2, period=5 ):
    if type( mat ) is float:
        return mat
    return mat1.rolling( window=period, min_periods=5 ).corr( mat2 )

def corr30M( mat1, mat2, period=30 ):
    if type( mat ) is float:
        return mat
    return mat1.rolling( window=period, min_periods=5 ).corr( mat2 )

def std5M( mat, period=5 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).std()

def std30M( mat, period=30 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).std()

def skew5M( mat, period=5 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).skew()

def skew30M( mat, period=30 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).skew()

def kurt5M( mat, period=5 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).kurt()

def kurt30M( mat, period=30 ):
    if type( mat ) is float:
        return mat
    return mat.rolling( window=period, min_periods=5 ).kurt()

def smmaM( mat, period=15 ):
    if type( mat ) is float:
        return mat
    return ( mat.shift( 1 ) * period - mat.shift( period - 1 ) + mat ) / period;

def rsiM( mat, period=15 ):
    if type( mat ) is float:
        return mat
    delta = mat.diff()
    dUp, dDown = delta.copy(), delta.copy()
    dUp[ dUp < 0 ] = 0
    dDown[ dDown > 0 ] = 0
    dDown = -dDown

    RolUp = dUp.rolling( window=period, min_periods=0 ).mean()
    RolDown = dDown.rolling( window=period, min_periods=0 ).mean()
    RS = RolUp / RolDown
    return 100 - 100/( 1 + RS )

def rankM( mat ):
    if type( mat ) is float:
        return mat
    mat_ranked = ( mat ).rank( axis=1 )
    mat_norm = mat_ranked.div( mat_ranked.max( axis=1 ) + 1, axis=0 ) * 100
    return mat_norm

def ventileM( mat, q=0.05 ):
    if type( mat ) is float:
        return mat
    mat_0 = DataFrame( 0, index=mat.index, columns=mat.columns )
    mat_lower = mat_0.add( mat.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat.quantile( 1 - q, axis=1), axis=0 )
    mat2 = mat.copy()
    mat2[ ( mat <= mat_lower ) | ( mat >= mat_upper ) ] = 1
    mat2[ ( mat > mat_lower ) & ( mat < mat_upper ) ] = np.nan
    return mat2

def decileM( mat, q=0.1 ):
    if type( mat ) is float:
        return mat
    mat_0 = DataFrame( 0, index=mat.index, columns=mat.columns )
    mat_lower = mat_0.add( mat.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat.quantile( 1 - q, axis=1), axis=0 )
    mat2 = mat.copy()
    mat2[ ( mat <= mat_lower ) | ( mat >= mat_upper ) ] = 1
    mat2[ ( mat > mat_lower ) & ( mat < mat_upper ) ] = np.nan
    return mat2

def quintileM( mat, q=0.2 ):
    if type( mat ) is float:
        return mat
    mat_0 = DataFrame( 0, index=mat.index, columns=mat.columns )
    mat_lower = mat_0.add( mat.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat.quantile( 1 - q, axis=1), axis=0 )
    mat2 = mat.copy()
    mat2[ ( mat <= mat_lower ) | ( mat >= mat_upper ) ] = 1
    mat2[ ( mat > mat_lower ) & ( mat < mat_upper ) ] = np.nan
    return mat2

def tercileM( mat, q=0.33333 ):
    if type( mat ) is float:
        return mat
    mat_0 = DataFrame( 0, index=mat.index, columns=mat.columns )
    mat_lower = mat_0.add( mat.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat.quantile( 1 - q, axis=1), axis=0 )
    mat2 = mat.copy()
    mat2[ ( mat <= mat_lower ) | ( mat >= mat_upper ) ] = 1
    mat2[ ( mat > mat_lower ) & ( mat < mat_upper ) ] = np.nan
    return mat2


def demean( group, b_devol=None ):
    if b_devol is None:
        b_devol = solbasic.sync_mod_attr( b_devol, cfg, "b_devol" )
    if b_devol:
        return ( group - group.mean() ) / group.std()
    else:
        return group - group.mean()


def neutralize( mat, mode=0 ):
    """ neutralize the positions. 0: by market, 1: by sector, 2: by group, 3: by subgroup, 9: by customized group. See comments for more modes """
    if type( mat ) is float:
        return mat
    gr_filter = lambda group: len( group ) >= cfg.n_min_group
    if mode == None:
        mode = cfg.d_configs[ "mode_neutral" ]
    #self.expr = "neutralize( " + self.expr + ", {} )".format( mode )
    if mode == 0:
    # Neutral by market
        mat2 = mat.sub( mat.mean( axis=1 ), axis=0 )
        return mat2
    # Neutral by GICS sector
    elif mode == 1:
        grouped = mat.T.groupby( by=gicsg.gsector.ix[ -1 ] ).filter( gr_filter ).groupby( by=gicsg.gsector.ix[ -1 ] )
    # Neutral by GICS group
    elif mode == 2:
        grouped = mat.T.groupby( by=gicsg.gind.ix[ -1 ] ).filter( gr_filter ).groupby( by=gicsg.gind.ix[ -1 ] )
    # Neutral by GICS subgroup
    elif mode == 3:
        grouped = mat.T.groupby( by=gicsg.gsubind.ix[ -1 ] ).filter( gr_filter ).groupby( by=gicsg.gsubind.ix[ -1 ] )
    # Neutral by customized groups
    elif mode == 999:
        try:
            mat_by = pd.read_csv( cfg.path_custom_group, header=0, names=[ "group" ], sep="," ).replace( "-99999", np.nan ).group.dropna().astype( int ).astype( str )
            mat_by.index = [ unicode(x) for x in mat_by.index ]
        except Exception as e:
            mat_by = DataFrame(1)
        mat_by.index.name = cfg.iden_stock
        glb.mat_by = mat_by.copy()
        grouped = mat.T.groupby( by=mat_by ).filter( gr_filter ).groupby( by=mat_by )
        glb.grouped = grouped
    else:
        solbasic.logger.error( "Mode unsupported." )
    mat2 = grouped.apply( demean ).T
    return mat2
    

def normalize( mat_alpha, book_size=None ):
    """ normalize to book size """
    book_size = solbasic.sync_mod_attr( book_size, cfg, "book_size" )
    return mat_alpha.div( ( np.abs( mat_alpha ) ).sum( axis=1 ), axis=0 ) * book_size


def truncate( mat, frac=None ):
    """ truncate the max position to be frac of total abs position.     """
    frac = solbasic.sync_mod_attr( frac, cfg, "f_trun_frac" )
    mat2 = mat.copy()
    if frac < 0:
        frac = cfg.f_trun_frac
    sum_abs_row = np.abs( mat2 ).sum( axis=1 )
    max_abs_row = np.abs( mat2 ).max( axis=1 )
    for i in max_abs_row.index:
        if max_abs_row[i] <= frac * sum_abs_row[i] or np.isnan( max_abs_row[i] ):
            continue
        else:
            row = mat2.ix[i]
            n_row_valid = len( row.dropna() )
            cnt = 0
            while max_abs_row[i] > frac * 1.1 * sum_abs_row[ i ] and cnt < n_row_valid:
                i_max = row[ np.abs( row ) == max_abs_row[ i ] ].index[ 0 ]
                row[ i_max ] = ( sum_abs_row[i] - np.abs( row[ i_max ] ) ) * frac / ( 1 - frac ) * np.sign( row[ i_max ] )
                sum_abs_row[i] = np.abs( row ).sum()
                max_abs_row[i] = np.abs( row ).max()
                cnt += 1
                mat2.ix[i] = row
    return mat2


def truncate_tvr( mat, frac=None ):
    """ truncate the max turnover per stock lower / higher than pctl """
    frac = solbasic.sync_mod_attr( frac, cfg, "f_trun_frac_tvr" )
    if frac < 0:
        #solbasic.logger.warn( "frac={} invalid, set default {}.".format( frac, cfg.f_trun_frac ) )
        frac = cfg.f_trun_frac
    mat_tvr = mat - mat.shift(1)
    mat_tvr[ mat_tvr > frac ]  = frac
    mat_tvr[ mat_tvr < -frac ] = -frac
    mat2 = mat.shift(1) + mat_tvr
    return mat2


def winsorize( mat, q=None ):
    """ set extreme data points at two ends to q and 1-q percentile """
    q = solbasic.sync_mod_attr( q, cfg, "f_wins_pctl" )
    mat2 = mat.copy()
    mat_0 = DataFrame(0, index=mat.index, columns=mat.columns)
    mat_lower = mat_0.add( mat.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat.quantile( 1 - q, axis=1), axis=0 )
    try:
        mat2[ mat < mat_lower ] = mat_lower[ mat < mat_lower ]
        mat2[ mat > mat_upper ] = mat_upper[ mat < mat_upper ]
    except TypeError:
        glb.logger.warn( "Unable to winsorize. Type of mat: {}.".format( type( mat2 ) ) )
    return mat2


def winsorize_tvr( mat, q=None ):
    """ truncate the max turnover per stock lower / higher than pctl """
    q = solbasic.sync_mod_attr( q, cfg, "f_wins_pctl_tvr" )
    mat2 = mat.copy()
    mat_tvr = mat2 - mat2.shift(1)
    mat_0 = DataFrame(0, index=mat_tvr.index, columns=mat_tvr.columns)
    mat_lower = mat_0.add( mat_tvr.quantile( q, axis=1), axis=0 )
    mat_upper = mat_0.add( mat_tvr.quantile( 1 - q, axis=1), axis=0 )
    mat_tvr[ mat_tvr < mat_lower ] = mat_lower[ mat_tvr < mat_lower ]
    mat_tvr[ mat_tvr > mat_upper ] = mat_upper[ mat_tvr > mat_upper ]
    mat2 = mat2.shift(1) + mat_tvr
    return mat2


def _decay_linear( array, n ):
    """ linear decay on alpha positions """
    s = pd.Series( array )
    m = len( s )
    s_decay = Series( range( 1, m + 1 ) ) / ( m * ( m + 1 ) / 2 )
    return ( s_decay * s ).sum()


def decay( mat, n=None, mode="exponential" ):
    """ decay alpha positions by n days """
    n = solbasic.sync_mod_attr( n, cfg, "n_days_hold" )
    if mode == "unweighted":
        mat2 = mat.rolling( window=n, min_periods=0 ).mean()
    elif mode == "exponential":
        mat2 = mat.ewm( span=n, min_periods=0 ).mean()
    elif mode == "linear":
        mat2 = mat.rolling( window=n, min_periods=0 ).apply( _decay_linear, args=( n, ) )
    else:
        solbasic.logger.warn( "mode={} unsupported, try unweighted, exponential.".format( mode ) )
    return mat2
