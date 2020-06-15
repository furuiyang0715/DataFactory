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

    process(changes, change_addition, change_remove, change_recover, addition_sentence, recover_sentence,
            remove_sentence)


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

    process(changes, change_addition, change_remove, change_recover, addition_sentence, recover_sentence,
            remove_sentence)


def process(changes, change_addition, change_remove, change_recover, addition_sentence, recover_sentence, remove_sentence):
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