import sys
sys.path.append("./../")

from hkland_elistocks.configs import LOCAL
from hkland_elistocks.human_gene import HumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools

sh = HumanTools()
if LOCAL:
    sh.create_target_table()
sh._process()


zh = ZHHumanTools()
if LOCAL:
    zh.create_target_table()
zh._process()
