'''
select 
date_format(a.transactions_dt, '%d/%m/%Y') as data,
case
	when a.transactions_vl >= 0 then 'Credito'
	when a.transactions_vl < 0 then 'Debito'
end as DebCre,
coalesce (cast(concat(cast('B' as varchar), cast(b.broker_transactions_id as varchar)) as varchar),
cast(concat(cast('B' as varchar), cast(c.legacy_broker_id as varchar)) as varchar)) as Broker,
case
	when a.account_type_id = 1 then 'Conta Corrente'
	when a.account_type_id = 4 then 'Bloqueio Judicial'
	when a.account_type_id = 8 then 'MED'
	when a.account_type_id = 9 then 'Bloqueio Cautelar'
end as conta,
'OPERAÇÕES_DIA' as Fonte,
sum(a.transactions_vl) as Valor,
count(a.transactions_uuid) as contagem
FROM trusted.transactions.dbo_transactions as a
left join trusted.transactions.dbo_transactions_broker_transactions as b on a.transactions_uuid = b.transactions_uuid
left join trusted.transactions.dbo_transactions_legacy as c on a.transactions_uuid = c.transactions_uuid
where a.partition_dt >= date('2024-06-07') -- apenas para performance
and a.partition_dt <= date('2024-06-11') -- apenas para performance
and date_format(a.transactions_dt, '%d/%m/%Y') in ('08/06/2024','09/06/2024','10/06/2024') -- Data especifica
and c.partition_dt >= date('2024-06-07')
and c.partition_dt <= date('2024-06-11')
and a.account_type_id in (1,4,8,9)
group by 1,2,3,4,5



-- Para validar bater quantidade dos casos acima com a quatidade da query abaixo
select
date_format(a.transactions_dt, '%d/%m/%Y'),
--a.created_by_ds, 
count(a.transactions_uuid) as contagem
FROM trusted.transactions.dbo_transactions as a
where a.partition_dt >= date('2024-06-10') -- apenas para performance
and a.partition_dt <= date('2024-06-12') -- apenas para performance
and date_format(a.transactions_dt, '%d/%m/%Y') = '11/06/2024'
and a.account_type_id in (1,4,8,9)
group by 1

'''

# -----------------------------------

'''
-- Usando slq_server

USE TRANSACTIONS
GO

SELECT TOP 1 
	convert(char, tr.TRANSACTIONS_DT,103) as data,
	case 
		when tr.TRANSACTIONS_VL >= 0 then 'Credito' 
		when tr.TRANSACTIONS_VL < 0 then 'Debito'
	end as DebCre,
	coalesce (CONCAT('B', tbt.BROKER_TRANSACTIONS_ID),CONCAT('B', tl.LEGACY_BROKER_ID)) as Broker,
	case
		when tr.ACCOUNT_TYPE_ID = 1 then 'Conta Corrente'
		when tr.ACCOUNT_TYPE_ID = 4 then 'Bloqueio Judicial'
		when tr.ACCOUNT_TYPE_ID = 8 then 'MED'
		when tr.ACCOUNT_TYPE_ID = 9 then 'Bloqueio Cautelar'
	end as conta,
	'OPERAÇÕES_DIA' as Fonte,
	sum(tr.TRANSACTIONS_VL) as valor,
	count(tr.TRANSACTIONS_UUID) as contagem
FROM 
	TRANSACTIONS as tr WITH(NOLOCK)
	left join TRANSACTIONS_BROKER_TRANSACTIONS as tbt WITH(NOLOCK) on tr.TRANSACTIONS_UUID = tbt.TRANSACTIONS_UUID
	left join TRANSACTIONS_LEGACY as tl WITH(NOLOCK) ON tr.TRANSACTIONS_UUID = tl.TRANSACTIONS_UUID
WHERE
	tl.CREATED_AT_DT >= '2024-06-15'
	and tl.CREATED_AT_DT < '2024-06-17'
	and tr.TRANSACTIONS_DT >= '2024-06-16' -- Data especifica
	and tr.TRANSACTIONS_DT < '2024-06-17'
	and tr.ACCOUNT_TYPE_ID in (1,4,8,9)
GROUP BY
	convert(char, tr.TRANSACTIONS_DT,103),
	case 
		when tr.TRANSACTIONS_VL >= 0 then 'Credito' 
		when tr.TRANSACTIONS_VL < 0 then 'Debito'
	end,
	coalesce (CONCAT('B', tbt.BROKER_TRANSACTIONS_ID),CONCAT('B', tl.LEGACY_BROKER_ID)),
	case
		when tr.ACCOUNT_TYPE_ID = 1 then 'Conta Corrente'
		when tr.ACCOUNT_TYPE_ID = 4 then 'Bloqueio Judicial'
		when tr.ACCOUNT_TYPE_ID = 8 then 'MED'
		when tr.ACCOUNT_TYPE_ID = 9 then 'Bloqueio Cautelar'
	end


'''