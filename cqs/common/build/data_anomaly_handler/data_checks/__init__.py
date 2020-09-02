from os import path
from os.path import dirname, basename, isfile
import glob
modules = glob.glob( path.dirname( path.abspath(__file__) ) + '/check*.py' )
__all__ = [ basename(f)[:-3] for f in modules if isfile(f)]
