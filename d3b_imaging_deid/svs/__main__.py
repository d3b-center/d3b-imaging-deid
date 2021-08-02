import sys

from d3b_imaging_deid.svs import deid_aperio_svs


filepath = sys.argv[1]
print(filepath, deid_aperio_svs(filepath))

