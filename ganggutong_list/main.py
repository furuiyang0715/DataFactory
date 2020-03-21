import sys
sys.path.append('./../')

from ganggutong_list.merge_lc_shsccomponent import SHMergeTools
from ganggutong_list.merge_lc_zhsccomponent import ZHMergeTools

tool = SHMergeTools()
tool.start()

tool = ZHMergeTools()
tool.start()
