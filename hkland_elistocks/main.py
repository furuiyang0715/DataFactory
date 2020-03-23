import sys
sys.path.append("./../")

from hkland_elistocks.list_check import list_check
from hkland_elistocks.single_process import fix
from hkland_elistocks.configs import LOCAL
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools


def main():
    # sh = SHHumanTools()
    # if LOCAL:
    #     sh.create_target_table()
    # sh._process()
    #
    #
    # zh = ZHHumanTools()
    # if LOCAL:
    #     zh.create_target_table()
    # zh._process()


    fix()

    list_check()


main()
