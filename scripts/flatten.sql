SELECT txn.*
,acc.district_id
,acc.frequency acct_freq
,dst.*
,dsp.type disp_type
,crd.card_id
,crd.type card_type
,crd.issued card_issued
,loa.date loan_date
,loa.amount loan_amount
,loa.duration loan_duration
,loa.payments loan_payments
,loa.status loan_status
FROM public.transactions txn inner join public.accounts acc ON (txn.account_id = acc.account_id)
inner join public.districts dst ON (acc.district_id = dst.a1)
inner join public.dispositions dsp ON (txn.account_id = dsp.account_id)
inner join public.cards crd ON (crd.disp_id = dsp.disp_id)
left outer join public.loans loa ON (loa.account_id = txn.account_id)
order by txn.trans_id
;