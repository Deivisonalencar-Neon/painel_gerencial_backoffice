'''

select 
date_format(a.partition_dt, '%d/%m/%Y') as data,
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
where a.partition_dt >= date('2024-06-03')
and a.partition_dt < date('2024-06-07')-- Data especifica
and c.partition_dt >= date('2024-05-25')
and a.account_type_id in (1,4,8,9)
group by 1,2,3,4,5



-- Para validar bater quantidade dos casos acima com a quatidade da query abaixo
select
a.partition_dt,
--a.created_by_ds, 
count(a.transactions_uuid) as contagem
FROM trusted.transactions.dbo_transactions as a
--left join trusted.transactions.dbo_transactions_broker_transactions as b on a.transactions_uuid = b.transactions_uuid 
--left join trusted.transactions.dbo_transactions_legacy as c on a.transactions_uuid = c.transactions_uuid 
where a.partition_dt >= date('2024-06-03')
and a.partition_dt < date('2024-06-07')-- Data especifica
--and c.register_dt >= date('2024-04-25')
and a.account_type_id in (1,4,8,9)
group by 1




'''