codes_add_1 = [
    '''
     ['000032', '000785', '002015', '002351', '002459', '002541', '002552', '002793', '002803', '002837', '002955', 
     '002959', '002961', '002962', '002966', '300080', '300455', '300468', '300552', '300677', '300775', '300776', 
     '300777', '300782', '300783', '300785', '300788', '300793', '300799', '002201', '002641', '002706', '002756', 
     '002838', '002869', '002880', '300114', '300132', '300209', '300319', '300388', '300395', '300448', '300525', 
     '300526', '300573', '300579', '300590', '300603', '300604', '300653', '300657', '300659', '300662', '300709', 
     '300771']

    '''
    
    
    '000032', '000785', '002015', '002351', '002459', '002541', '002552', '002793', '002803', '002837', '002955',
    '002959', '002961', '002962', '002966', '300080', '300455', '300468', '300552', '300677', '300775', '300776',
    '300777', '300782', '300783', '300785', '300788', '300793', '300799', '002201', '002641', '002706', '002756',
    '002838', '002869', '002880', '300114', '300132', '300209', '300319', '300388', '300395', '300448', '300525',
    '300526', '300573', '300579', '300590', '300603', '300604', '300653', '300657', '300659', '300662', '300709',
    '300771',    # ok
]
codes_add_134 = [
    # ['000912', '002214', '300663', '002243', '002947', '300328']
    '000912', '002214', '300663', '002243', '002947', '300328',    # ok
]
codes_recover_1 = [
    '''
    ['000058', '002239', '002312', '002605', '300083', '300376', '002791', '000601', '000796', 
    '000903', '002083', '002126', '002135', '002324', '002479', '002484', '002528', '002609', 
    '002616', '002850', '002918', '300031', '300045', '300229', '300303', '300386', '300438', 
    '300477', '300568', '300571', '300607', '300613', '300623', '300624', '300664', '300672', 
    '300684', '300737']
    
    '''
    '000058', '002239', '002312', '002605', '300083', '300376', '002791', '000601', '000796',
    '000903', '002083', '002126', '002135', '002324', '002479', '002484', '002528', '002609',
    '002616', '002850', '002918', '300031', '300045', '300229', '300303', '300386', '300438',
    '300477', '300568', '300571', '300607', '300613', '300623', '300624', '300664', '300672',
    '300684', '300737',    # ok
]
codes_recover_134 = [
    '''
    ['000030', '000519', '000700', '000719', '000917', '002169', '002250', '002287', '000652', 
    '000823', '000829', '002022', '002079', '002106', '002117', '002161', '002182', '002276', 
    '002313', '002428', '002518', '300020', '300177', '300202', '300256', '300287', '300397']

    '''
    '000030', '000519', '000700', '000719', '000917', '002169', '002250', '002287', '000652',
    '000823', '000829', '002022', '002079', '002106', '002117', '002161', '002182', '002276',
    '002313', '002428', '002518', '300020', '300177', '300202', '300256', '300287', '300397',
]    # ok
codes_remove_1 = [
    '''
    ['000429', '000863', '002314', '000657', '000666', '000815', '000882', '002057', '002309', 
    '002550', '300185', '300252']

    '''
    '000429', '000863', '002314', '000657', '000666', '000815', '000882', '002057', '002309',
    '002550', '300185', '300252',    # ok
]
codes_remove_134 = [
    '''
    ['000088', '000552', '002280', '002293', '002370', '002608', '000040', '000525', '000980', 
    '002366', '300367', '000036', '000592', '000861', '000926', '000928', '002215', '002274', 
    '002378', '002639', '300266', '300355']

    '''
    '000088', '000552', '002280', '002293', '002370', '002608', '000040', '000525', '000980',
    '002366', '300367', '000036', '000592', '000861', '000926', '000928', '002215', '002274',
    '002378', '002639', '300266', '300355',    # ok


]


def process_sz_changes(changes):
    change_addition = 'Addition'
    change_remove = 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
    change_recover = 'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))'
    addition_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for \
Margin Trading and the List of Eligible SZSE Securities for Short Selling'
    recover_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for \
Margin Trading and the List of Eligible SZSE Securities for Short Selling as it is \
also included in SZSE stock list for margin trading and shortselling.'
    remove_sentence = 'This stock will also be removed from the List of Eligible SZSE Securities \
for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'
    add_134 = []
    add_1 = []

    recover_1 = []
    recover_134 = []

    remove_1 = []
    remove_134 = []
    for change in changes:
        _change, _remarks, secu_code = change.get('Ch_ange'), change.get("Remarks"), change.get("SSESCode")
        if _change == change_addition:
            if addition_sentence in _remarks:
                add_134.append(secu_code)
            else:
                add_1.append(secu_code)
        elif _change == change_recover:
            if recover_sentence in _remarks:
                recover_134.append(secu_code)
            else:
                recover_1.append(secu_code)
        elif _change == change_remove:
            if remove_sentence in _remarks:
                remove_134.append(secu_code)
            else:
                remove_1.append(secu_code)

    print("add_1: ", add_1)
    print("add_134: ", add_134)

    print("recover_1:", recover_1)
    print("recover_134: ", recover_134)

    print("remove_1: ", remove_1)
    print("remove_134: ", remove_134)


def process_sh_changes(changes):
    change_addition = 'Addition'
    change_remove = 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'
    change_recover = 'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))'

    addition_sentence = 'This stock will also be added to the List of Eligible SSE Securities for \
    Margin Trading and the List of Eligible SSE Securities for Short Selling'
    recover_sentence = 'This stock will also be added to the List of Eligible SSE Securities \
    for Margin Trading and the List of Eligible SSE Securities for Short Selling as it is \
    also included in SSE stock list for margin trading and shortselling.'
    remove_sentence = 'This stock will also be removed from the List of Eligible SSE Securities \
    for Margin Trading and the List of Eligible SSE Securities for Short Selling.'

    add_134 = []
    add_1 = []

    recover_1 = []
    recover_134 = []

    remove_1 = []
    remove_134 = []
    for change in changes:
        _change, _remarks, secu_code = change.get('Ch_ange'), change.get("Remarks"), change.get("SSESCode")
        if _change == change_addition:
            if addition_sentence in _remarks:
                add_134.append(secu_code)
            else:
                add_1.append(secu_code)
        elif _change == change_recover:
            if recover_sentence in _remarks:
                recover_134.append(secu_code)
            else:
                recover_1.append(secu_code)
        elif _change == change_remove:
            if remove_sentence in _remarks:
                remove_134.append(secu_code)
            else:
                remove_1.append(secu_code)

    print(add_1)
    print(add_134)

    print(recover_1)
    print(recover_134)

    print(remove_1)
    print(remove_134)
