'''

SELECT DISTINCT
a.PARTITION_DT as DATA,
case when a.TRANSACTIONS_VL >= 0 then 'Credito' when a.TRANSACTIONS_VL < 0 then 'Debito' end as DEB_CRED,
CONCAT('B', a.BROKER_TRANSACTIONS_ID) as brokerid,
b.TRANSITORY_ACCOUNT_DS as transitoria,
'TRANSITORIAS' as fonte,
sum(a.TRANSACTIONS_VL) as valor,
count(a.BROKER_TRANSACTIONS_ID) as quantidade
from POSITION_KEEPING.dbo.transitory_account_transactions as a (nolock)
inner join  POSITION_KEEPING.dbo.TRANSITORY_ACCOUNT as b (nolock) on a.TRANSITORY_ACCOUNT_UUID = b.TRANSITORY_ACCOUNT_UUID
where a.PARTITION_DT >= '2024-06-03'
and a.PARTITION_DT < '2024-06-07'
AND b.TRANSITORY_ACCOUNT_DS not in ('Geral - Transações desprezadas')
group by a.PARTITION_DT, case when a.TRANSACTIONS_VL >= 0 then 'Credito' when a.TRANSACTIONS_VL < 0 then 'Debito' end, a.BROKER_TRANSACTIONS_ID, b.TRANSITORY_ACCOUNT_DS




SELECT DISTINCT
CONVERT(char, a.MOVEMENT_DT, 103) as RegisterDate,
case when a.MOVEMENT_VL >= 0 then 'Credito' when a.MOVEMENT_VL < 0 then 'Debito' end as DEB_CRED,
CONCAT('B', a.BROKER_TRANSACTIONS_ID) as brokerid,
b.TRANSITORY_ACCOUNT_DS as transitoria,
'TRANSITORIAS' as fonte,
sum(a.MOVEMENT_VL) as valor,
count(a.BROKER_TRANSACTIONS_ID) as quantidade
from POSITION_KEEPING.dbo.transitory_account_movement as a (nolock)
inner join  POSITION_KEEPING.dbo.TRANSITORY_ACCOUNT as b (nolock) on a.TRANSITORY_ACCOUNT_UUID = b.TRANSITORY_ACCOUNT_UUID
where a.MOVEMENT_DT >= '2023-06-03 00:00:00.000'
and a.MOVEMENT_DT < '2023-06-07 00:00:00.000'
AND b.TRANSITORY_ACCOUNT_DS not in ('Geral - Transações desprezadas')
group by CONVERT(char, a.MOVEMENT_DT, 103), case when a.MOVEMENT_VL >= 0 then 'Credito' when a.MOVEMENT_VL < 0 then 'Debito' end, a.BROKER_TRANSACTIONS_ID, b.TRANSITORY_ACCOUNT_DS



'''