import sys
sys.path.append('./../')

from hkland_component.merge_lc_shsccomponent import SHMergeTools
from hkland_component.merge_lc_zhsccomponent import ZHMergeTools


def main():
    sh = SHMergeTools()
    sh.start()

    zh = ZHMergeTools()
    zh.start()

main()
