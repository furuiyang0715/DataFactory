import sys
sys.path.append('./../')

from hkland_component.merge_lc_shsccomponent import SHMergeTools
from hkland_component.merge_lc_zhsccomponent import ZHMergeTools

tool = SHMergeTools()
tool.start()

tool = ZHMergeTools()
tool.start()
