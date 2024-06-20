'''
USE POSITION_KEEPING
GO
DECLARE @dt_part_i DATE = '2024-04-30'; -- D-1
DECLARE @dt_efet_i DATE = '2024-05-01'; -- Data inicio
DECLARE @dt_efet_f DATE = '2024-06-17'; -- Data fim (considerar +1)
SELECT
	convert(char, t.TRANSACTIONS_DT, 103) as DATA,
	case 
		when t.TRANSACTIONS_VL >= 0 then 'Credito' 
		when t.TRANSACTIONS_VL < 0 then 'Debito' 
	end as DEB_CRED,
	CONCAT('B', t.BROKER_TRANSACTIONS_ID) as brokerid,
	c.TRANSITORY_ACCOUNT_DS as transitoria,
	'TRANSITORIAS' as fonte,
	sum(t.TRANSACTIONS_VL) as valor,
	count(t.BROKER_TRANSACTIONS_ID) as quantidade
from 
	POSITION_KEEPING.dbo.transitory_account_transactions as t with (nolock)
	inner join POSITION_KEEPING.dbo.TRANSITORY_ACCOUNT as c (nolock) on t.TRANSITORY_ACCOUNT_UUID = c.TRANSITORY_ACCOUNT_UUID
where 
	-- Apenas para performance
	t.partition_dt >= @dt_part_i
    and t.partition_dt <= @dt_efet_f
	-- Datas que de fato serão extraidas
    and t.TRANSACTIONS_DT >= @dt_efet_i
	and t.TRANSACTIONS_DT < @dt_efet_f
	and c.TRANSITORY_ACCOUNT_DS not in ('Geral - Transações desprezadas')
group by 
	convert(char, t.TRANSACTIONS_DT, 103), 
	case 
		when t.TRANSACTIONS_VL >= 0 then 'Credito' 
		when t.TRANSACTIONS_VL < 0 then 'Debito' 
	end, 
	t.BROKER_TRANSACTIONS_ID,
	c.TRANSITORY_ACCOUNT_DS
UNION SELECT
	CONVERT(char, m.created_at_dt, 103) as RegisterDate,
	case 
		when m.MOVEMENT_VL >= 0 then 'Credito' 
		when m.MOVEMENT_VL < 0 then 'Debito' 
	end as DEB_CRED,
	CONCAT('B', m.BROKER_TRANSACTIONS_ID) as brokerid,
	c.TRANSITORY_ACCOUNT_DS as transitoria,
	'TRANSITORIAS' as fonte,
	sum(m.MOVEMENT_VL) as valor,
	count(m.BROKER_TRANSACTIONS_ID) as quantidade
from 
	POSITION_KEEPING.dbo.transitory_account_movement as m with(nolock)
	inner join  POSITION_KEEPING.dbo.TRANSITORY_ACCOUNT as c with(nolock) on m.TRANSITORY_ACCOUNT_UUID = c.TRANSITORY_ACCOUNT_UUID
where 
	m.created_at_dt >= @dt_efet_i
	and m.created_at_dt < @dt_efet_f
	and c.TRANSITORY_ACCOUNT_DS not in ('Geral - Transações desprezadas')
group by 
	CONVERT(char, m.created_at_dt, 103), 
	case 
		when m.MOVEMENT_VL >= 0 then 'Credito' 
		when m.MOVEMENT_VL < 0 then 'Debito' 
	end, 
	m.BROKER_TRANSACTIONS_ID, 
	c.TRANSITORY_ACCOUNT_DS

'''