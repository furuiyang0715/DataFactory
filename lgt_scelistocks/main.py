import sys
sys.path.append("./../")

from lgt_scelistocks.configs import LOCAL
from lgt_scelistocks.human_gene import HumanTools
from lgt_scelistocks.zh_human_gene import ZHHumanTools

# sh = HumanTools()
# if LOCAL:
#     sh.create_target_table()
# sh._process()


zh = ZHHumanTools()
if LOCAL:
    zh.create_target_table()
zh.process()
