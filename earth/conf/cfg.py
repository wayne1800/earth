
""" program wide configurations """

import sys
from sol.core import date as soldate


######################
# simulation configs #
######################

sim_period = "forward" #"custom" "test" "long" "full" "forward" "live" # see bottom for presets
sim_mode = "cache" # "cache", "alpha", "combo", "plot", "comboplot"
dataset = "cs" #"gic" "cs" "base"
db = "xf"
b_alphagen_on = False # random alpha generation flag
b_alpha_combo = False # compute alpha combo pnl with tdate x aid instead of tdate x useqid
b_combo = False # compute alpha combo. Requires b_process_alpha_dump True
b_init_filter = False # conduct IS, VS tests for existing pool
b_load_init_pop = False #not b_init_filter # load initial population in final pool for correlation test. set true for correlation test against existing pool.
b_override_mode_neutral = False # set alphagen mode_neutral as the one in this config. True for alphagen.
n_id_skip_till = 0 # if b_init_filter True, start screening from this alpha id
b_load_cache_from_db = False # load alpha cache from path_db or not
b_process_alpha_dump = False # load and process alpha stats and positions from alpha folder. Available only after daily dump.
b_plot = False # plot pnl for alphas in cache
b_conduct_tests = False # run IS, VS tests in filter. Only in force when b_init_filter True. Set False for sim_period in ["forward","live"] if necessary
b_conduct_corr_screen = True # run corr test in filter. Only in force when b_init_filter True. Set False for sim_mode "corrscreen"
b_recompute_alpha = True # recompute alpha or load it from truncated pool. Required in alpha generation and refresh. Set false to pass same alphas through
b_recreate_cache = False #True: force recreate cache, False: create or load from cache
b_refresh_cache = False #True: refresh cache
n_days_refresh = 10 # number of days to refresh if b_refresh_cache true
b_recreate_alpha = False #True: force recreate alpha, False: create or load from alpha
b_recreate_alpha_cache = False #True: force recreate alpha cache (pdata), False: create if absent, or load from existing cache
b_refresh_alpha_cache = False #True: refresh alpha cache
b_dryrun = False # True: test run without writing to cache or alphas
b_prepare_data = True # reload data from cache and set top universe. Set False for interactive mode if data already loaded
b_keep_id = True # respect existing alpha id. Set False to reassign all ids. False in alphagen mode.
b_keep_gen = False # Keep alpha generation after filtering. True for alpha generation


##########
# consts #
##########

start_date       = "20050101" # simulation start date + back days
end_date         = "20131231" # simulation end date
b_dump_pos       = True # keep it so to optimize memory usage
b_dump_pos_only  = False # dump alpha daily position, not full stats
b_skip_daily_pos = False # True: skip daily position dump in alpha folder
mode_pnl         = 0 # 0: close based pnl. Can expand if needed
mode_tcost       = 1 # 0: before-cost calculation, 1: after cost
tcost50          = 0.001 # one-way cost. Temporary parameter before spread is available
mode_neutral     = 3 # 0: market, 1: by sector, 2: by group, 3: by subgroup, 999: custom group in path_custom_group 
leverage         = 2
n_days_buffer    = 50 # days to on top of loaded backdays for buffering
n_days_dump      = 15 # days to dump alpha position when running live
n_days_decay     = 0 # window in n days to linearly decay
commission       = 0.000075 # commission per share
n_in_play        = 1000
l_n_in_play      = [ 100, 200, 300, 500, 750, 1000 ] #, 2000 ]
mode_in_play     = 2
delay            = 1
book_size        = 1


#################
# alpha configs #
#################

sim_days_start  = 2000 # simulation range. set 0: start from first loaded day
sim_days_is_vs  = 750  # cutoff between in-sample and validation sample
sim_days_end    = 1 # set 1: thru the last load day
load_days_start = 0 # take last start thru end days as simulation period. set 0: start from first store day
load_days_end   = 1 # set 1: thru the last store day
load_backdays   = 252 # allow backdays before simulation period
n_min_group     = 2 # min instruments in group. 
n_bdays         = 252
b_devol         = True #True: devide by volatility per cluster in neutralization
l_alph_manual = [] # alpha expressions supplied manually


################
# plot configs #
################

fig_size_combo = 20. # figure width for efficient frontier series charts


#################
# cache configs #
#################

f_price_lower = 10
f_price_upper = 1500
min_vol       = 50000
min_cap       = 150
iden_stock    = "cusip" #"isin" "cusip" "gvkey"
iden_batch    = "alpha"
instrument    = "equity"
region        = "us"
path_folder   = "/mnt/data2/{}/{}/".format( instrument, region )
path_cache    = "{}cache/{}/".format( path_folder, iden_stock )
path_custom_group = ""


####################
# alphagen configs #
####################
n_pop = 2
n_processes = 3 # number of processes. Max number for alphagen mode


################
# data configs #
################

path_data      = "{}data/".format( path_folder )
path_data_ws   = "{}ws/".format( path_data )
path_data_de   = "{}de/".format( path_data )
path_data_ibes = "{}ibes/".format( path_data )


##################################
# selection rules and thresholds #
##################################

f_trun_frac       = 0.1  # truncate fraction
f_trun_frac_tvr   = 0.005  # truncate fraction
f_wins_pctl_tvr   = 0.05  # max tvr per instrument
f_wins_pctl       = 0.02 # winsorize percentile
f_thold_p_is      = 0.2 # in-sample two-tailed p value  
f_thold_p_vs      = 0.2 # test-sample two-tailed p value  
n_days_corr_pnl   = 504 # n days for position and trade correlation test
n_days_corr_pt    = 30 # n days for position and trade correlation test
f_thold_tvr       = 250 # tvr % threshold
f_thold_corr      = 0.8 # corr threshold
f_thold_lsimb     = 0.6 # long-short balance threshold
f_thold_dump_days = 0.2 # threshold days to dump at least one instrument
f_thold_inst      = 0.1 # threshold number of instruments dumped at least once in simulation period 


##############
# Py configs #
##############

niceness    = 12 # default niceness
max_rows    = 50 # max rows to display df in full
max_columns = 10 # max rows to display df in full
n_verbose_prog = 100 # report progress every n_verbose_prog iterations
mem_thold = 0.5 # threshold to stop submission and hold alphagen
d_mem_proc = { (0,0.1):10, (0.1,0.2):6, (0.2,mem_thold):3, (mem_thold,1):0 } # proper number of processes given memory usage range
l_periods_halt = [ (5.5, 10) ] # halt alphagen in these slots


# dynamic config reload
def reload( mod ):
    # Cache mode, load data only
    if mod.sim_mode == "cache":
        mod.b_alphagen_on = False
    # Alpha daily refresh mode, load data and refresh alphas within the same batch
    elif mod.sim_mode == "alpharef":
        mod.b_alphagen_on = True
        mod.b_prepare_data = False
        mod.b_init_filter = True
        mod.b_conduct_tests = False
        mod.b_conduct_corr_screen = False
        mod.b_process_alpha_dump = False
        mod.b_recreate_alpha = False
        mod.b_plot = False
    # Alpha generation mode, load data, generate alphas and run tests
    elif mod.sim_mode == "alphagen":
        mod.b_alphagen_on = True
        mod.b_prepare_data = False
        mod.b_conduct_tests = True
        mod.b_conduct_corr_screen = True
        mod.b_keep_gen = True
        mod.b_keep_id = False
        mod.b_override_mode_neutral = True
        mod.b_recreate_alpha = True
        mod.b_skip_daily_pos = True
    # Plot mode, load alpha stats and positions from alpha cache, dump pnl, cov matrix and plot
    elif mod.sim_mode == "plot":
        mod.b_prepare_data = False
        mod.b_process_alpha_dump = True
        mod.b_plot = True
    # Combo plot mode, load combo stats and positions from combo stats cache and plot
    elif mod.sim_mode == "comboplot":
        mod.b_prepare_data = False
        mod.b_process_alpha_dump = False
        mod.b_combo = False
        mod.b_plot = True

    # preset simulation modes
    if mod.sim_period == "test":
        mod.start_date = "20120101"
        mod.end_date   = "20131231"
        mod.sim_days_start  = 0
        mod.sim_days_is_vs  = 50
    elif mod.sim_period == "long":
        mod.start_date = "20050101"
        mod.end_date   = "20131231"
        mod.sim_days_start  = 2000
        mod.sim_days_is_vs  = 750
        mod.n_processes = 3
        mod.n_workers = 3
    elif mod.sim_period == "full":
        mod.start_date = "20050101"
        mod.end_date = soldate.date_add( -mod.delay )
        mod.sim_days_start  = 2500
        mod.sim_days_is_vs  = 1250
        mod.n_processes = 3
        mod.n_workers = 3
    # forward test
    elif mod.sim_period == "forward":
        mod.start_date = "20120101"
        mod.end_date   = soldate.date_add( -mod.delay )
        mod.sim_days_start  = 450
        mod.sim_days_is_vs  = 150
    # live, for recent cache refresh and alpha dump
    elif mod.sim_period == "live":
        mod.start_date = "20120101"
        mod.end_date = soldate.date_add( -mod.delay )
        mod.sim_days_start  = 450
        mod.sim_days_is_vs  = 150
        mod.niceness = 10

    # storage with identifier
    if mod.sim_period in [ "forward", "live" ]:
        mod.name_batch = "{}_top{}_{}_forward".format( mod.dataset, mod.n_in_play, mod.start_date )
    else:
        mod.name_batch = "{}_top{}_{}_{}".format( mod.dataset, mod.n_in_play, mod.start_date, mod.end_date )
    mod.target_dir = "{}{}/d{}/{}/{}/".format( mod.path_folder, mod.iden_batch, mod.delay, mod.mode_neutral, mod.name_batch )
    mod.str_alpha_position = mod.target_dir + "pool/"
    mod.str_db_position = mod.str_alpha_position + "db.csv"
    mod.fname_cp = mod.target_dir + "pool.pkl"
    mod.fname_cp_init = mod.target_dir + "pool_init.pkl"
    mod.fname_cp_final = mod.target_dir + "pool_final.pkl"
    
    # combo related paths
    mod.path_repo = mod.target_dir 
    mod.path_combo = "{}combo/".format( mod.path_repo )
    mod.str_db_position_combo = "{}/db.csv".format( mod.path_repo ) # alpha db for combo
    mod.path_db = "{}db/main.db".format( mod.path_folder )
    mod.str_db_cond = "irr > 0.4"
    mod.path_alpha_pool = mod.path_repo + "pool.pkl" 
    mod.path_alpha_pool_init = mod.path_repo + "pool_init.pkl" 
    mod.path_alpha_pool_final = mod.path_repo + "pool_final.pkl" 

    # alpha configs summary
    mod.d_configs={ "mode_neutral":mod.mode_neutral, "mode_pnl":mod.mode_pnl, "mode_tcost":mod.mode_tcost, "delay":mod.delay, "n_in_play":mod.n_in_play, "start_date":mod.start_date, "end_date":mod.end_date, "sim_days_start":mod.sim_days_start, "sim_days_end":mod.sim_days_end, "load_backdays":mod.load_backdays, "book_size":mod.book_size, "f_wins_pctl":mod.f_wins_pctl, "f_trun_frac":mod.f_trun_frac, "n_days_decay":mod.n_days_decay, "n_days_corr_pt":mod.n_days_corr_pt, "n_days_corr_pnl":mod.n_days_corr_pnl }
    
    # variable relations
    if mod.sim_period == "test":
        mod.sec_sleep = 10 # init wait time in seconds before multi-processing
    else:
        mod.sec_sleep = mod.n_processes * 10


# append preset simulation mode to config
reload( sys.modules[ __name__ ] )
