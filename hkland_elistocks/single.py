import sys

sys.path.append("./../")
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools


class DailyUpdate(object):

    def run_0615(self):
        '''
        sh_add_1:  ['600070', '600984', '601512', '601816', '603053', '603068', '603218', '603489', '603520',
                    '603610', '603690', '603786', '603920', '603927', '603960']
        sh_add_134:  ['600131', '600223', '600529', '600764', '601519', '603012', '603018', '603601', '603678']
        sh_recover_1:  ['600988']
        sh_recover_134:  ['600079', '600143', '600621', '600737', '600776', '600802', '603000']
        sh_remove_1:  ['600693', '603007', '603080', '603165', '603332', '603339', '603351', '603603', '603773',
                        '603877', '603897', '603898']
        sh_remove_134 ['600123', '600230', '600231', '600239', '600297', '600398', '600418', '600499', '600528',
                        '600535', '600623', '600661', '600664', '600771', '600826', '600986', '601002', '601222',
                        '601997', '603959']

        sz_add_1:  ['000032', '000785', '002015', '002351', '002459', '002541', '002552', '002793', '002803',
                    '002837', '002955', '002959', '002961', '002962', '002966', '300080', '300455', '300468',
                    '300552', '300677', '300775', '300776', '300777', '300782', '300783', '300785', '300788',
                    '300793', '300799', '002201', '002641', '002706', '002756', '002838', '002869', '002880',
                    '300114', '300132', '300209', '300319', '300388', '300395', '300448', '300525', '300526',
                    '300573', '300579', '300590', '300603', '300604', '300653', '300657', '300659', '300662',
                    '300709', '300771']
        sz_add_134:  ['000912', '002214', '300663', '002243', '002947', '300328']
        sz_recover_1:  ['000058', '002239', '002312', '002605', '300083', '300376', '002791', '000601', '000796',
                        '000903', '002083', '002126', '002135', '002324', '002479', '002484', '002528', '002609',
                        '002616', '002850', '002918', '300031', '300045', '300229', '300303', '300386', '300438',
                        '300477', '300568', '300571', '300607', '300613', '300623', '300624', '300664', '300672',
                        '300684', '300737']
        sz_recover_134:  ['000030', '000519', '000700', '000719', '000917', '002169', '002250', '002287', '000652',
                        '000823', '000829', '002022', '002079', '002106', '002117', '002161', '002182', '002276',
                        '002313', '002428', '002518', '300020', '300177', '300202', '300256', '300287', '300397']
        sz_remove_1:  ['000429', '000863', '002314', '000657', '000666', '000815', '000882', '002057', '002309',
                        '002550', '300185', '300252']
        sz_remove_134 ['000088', '000552', '002280', '002293', '002370', '002608', '000040', '000525', '000980',
                        '002366', '300367', '000036', '000592', '000861', '000926', '000928', '002215', '002274',
                        '002378', '002639', '300266', '300355']
        '''
        base_sql = """select * from hkland_hgelistocks where SecuCode = '{}' order by InDate;"""
        for code in ['600070', '600984', '601512', '601816', '603053', '603068', '603218', '603489', '603520',
                    '603610', '603690', '603786', '603920', '603927', '603960']:
            sql = base_sql.format(code)
            print(sql)


            pass

    def refresh_time(self):
        sh = SHHumanTools()
        zh = ZHHumanTools()
        sh.refresh_update_time()
        zh.refresh_update_time()

    def start(self):
        self.run_0615()

        self.refresh_time()


if __name__ == "__main__":
    dp = DailyUpdate()
    dp.start()
