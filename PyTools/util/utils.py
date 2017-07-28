
""" util functions """

import os
import logging
import subprocess
import pipes


def _append_ns_list( ns, target, source ):
    """ append source to target in namespace list. Mutables must be appended this way in multiprocessing """
    l_temp = getattr( ns, target )
    if type( l_temp ) == list:
        l_temp.append( source )
        setattr( ns, target, l_temp )


def _append_ns_set( ns, target, source ):
    """ append source to target in namespace set. Mutables must be appended this way in multiprocessing """
    l_temp = getattr( ns, target )
    if type( l_temp ) == set:
        l_temp.add( source )
        setattr( ns, target, l_temp )


def _update_ns_list( ns, target, idx, source ):
    """ replace target[idx] with source in namespace """
    l_temp = getattr( ns, target )
    if type( l_temp ) == list:
        l_temp[ idx ] = source
        setattr( ns, target, l_temp )


def _pop_ns_list( ns, target ):
    """ pop last element of target list in namespace """
    l_temp = getattr( ns, target )
    if type( l_temp ) == list:
        elem = l_temp.pop()
        setattr( ns, target, l_temp )
    return elem


def _append_ns_dict( ns, target, key, value ):
    """ add to target dictionary in namespace. mutables must be appended this way in multiprocessing """
    d_temp = getattr( ns, target )
    if type( d_temp ) == dict:
        d_temp[ key ] = value
        setattr( ns, target, d_temp )


def _incr_ns_dict( ns, target, key, value ):
    """ increment target dictionary in namespace with key by value. """
    d_temp = getattr( ns, target )
    if type( d_temp ) == dict:
        if key not in d_temp.keys():
            d_temp[ key ] = value
        elif type( d_temp[ key ] ) is int or type( d_temp[ key ] ) is float:
            d_temp[ key ] += value
        setattr( ns, target, d_temp )


def mem_usage( keyword="", mem_deduct=0 ):
    """ check system memory usage 
    keyword: if specified, deduct memory taken by processes containing keyword
    mem_deduct: ad-hoc used memory deduction
    """
    out = subprocess.Popen( [ "free" ], stdout=subprocess.PIPE).communicate()[0].split(b'\n')
    index_tot = out[0].split().index(b'total') + 1
    mem_tot = float( out[1].split()[ index_tot ] )
    index_used = out[0].split().index(b'used') + 1
    mem_used = float( out[1].split()[ index_used ] )
    if keyword:
        out = subprocess.Popen( "smem -P {} -t".format( keyword ).split(), stdout=subprocess.PIPE).communicate()[0].split(b'\n')
        mem_used -= float( out[-2].split()[-2] )
    if mem_deduct > 0:
        mem_used -= mem_deduct
    if mem_used < 0:
        mem_used = 1.
    return mem_used / mem_tot


def owner( pid ):
        '''Return username of UID of process pid'''
        UID   = 1
        EUID  = 2
        f_status = "/proc/{}/status".format( pid ) 
        usr_name = ''
        if os.path.exists( f_status ):
            for ln in open( '/proc/{}/status' % pid ):
                if ln.startswith( 'Uid:' ):
                    uid = int( ln.split()[ UID ] )
                    usr_name = pwd.getpwuid( uid ).pw_name
        return usr_name
    
    
def rsync_file( source, target ):
    """ rsync source to target """
    cmd = "rsync -tr {} {}".format( source, target )
    result = subprocess.call( cmd.split() )
    if result == 0:
        return True
    elif result == 1:
        logging.warn( "Failed to rsync {} to {}.".format( source, target ) )
        return False
    else:
        logging.critical( "Invalid input for rsync." )
        return False


def remove_file( source ):
    """ remove source """
    cmd = "rm -rf {}".format( source )
    result = subprocess.call( cmd.split() )
    if result == 0:
        return True
    elif result == 1:
        logging.warn( "Failed to remove {}.".format( source ) )
        return False
    else:
        logging.critical( "Invalid input for rm." )
        return False


def exists_remote( host, path ):
    """ Test if a file exists at path on a host accessible with SSH """
    status_f = subprocess.call( [ 'ssh', host, 'test -f {}'.format( pipes.quote( path ) ) ] )
    status_d = subprocess.call( [ 'ssh', host, 'test -d {}'.format( pipes.quote( path ) ) ] )
    status = status_f * status_d
    if status == 0:
        return True
    if status == 1:
        return False
    raise Exception( 'SSH failed' )
