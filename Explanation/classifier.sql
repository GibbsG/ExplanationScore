-- this is the classifier with pre-bucket data
create OR REPLACE function classifier2(
	f1 int,
	f2 int,
	f3 int,
	f4 int,
	f5 int,
	f6 int,
	f7 int,
	f8 int,
	f9 int,
	f10 int,
	f11 int,
	f12 int,
	f13 int,
	f14 int,
	f15 int,
	f16 int,
	f17 int,
	f18 int,
	f19 int,
	f20 int,
	f21 int,
	f22 int,
	f23 int
)
returns int as $$
DECLARE
index_1 int := 0;
index_2 int[3];
index_3 int := 0;
index_4 int[4];
index_5 int[4];
index_6 int[3];
index_7 int[3];
index_8 int[2];
index_9 int := 0;
index_10 int := 0;
score float := -8.3843046;
begin
index_1:= f1;
index_2[1] = f2;
index_2[2] = f3;
index_2[3] = f4;
index_3:= f5;
index_4[1] = f6;
index_4[2] = f7;
index_4[3] = f12;
index_4[4] = f13;
index_5[1] = f8;
index_5[2] = f9;
index_5[3] = f10;
index_5[4] = f11;
index_6[1] = f14;
index_6[2] = f19;
index_6[3] = f21;
index_7[1] = f15;
index_7[2] = f16;
index_7[3] = f17;
index_8[1] = f18;
index_8[2] = f20;
index_9:= f22;
index_10:= f23;
score := score + external_risk_subscore_from_index(index_1) + trade_open_time_subscore_from_index(index_2)
        + num_sat_trades_subscore_from_index(index_3) + trade_freq_subscore_from_index(index_4)
        + delinquency_subscore_from_index(index_5) + installment_subscore_from_index(index_6)
        + inquiry_subscore_from_inedx(index_7) + revol_balance_subscore_from_index(index_8)
        + utilization_subscore_from_index(index_9) + trade_w_balance_subscore_from_index(index_10);
score:= 1 / (1+exp(-score));
IF (score < 0.5) THEN
	return 0;
ELSE
	return 1;
END IF;
end;
$$
language plpgsql;
