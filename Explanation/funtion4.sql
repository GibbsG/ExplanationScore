create OR REPLACE function function4(
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


  demisions int [] := '{4, 3, 1, 3, 4, 4, 3, 4, 3, 2, 1, 4, 4, 4, 4, 3, 1, 3, 2, 4, 4, 4, 4}';
  feature_ranges int[][] := '{{64,71,76,81}, {92,135,264, 0}, {20, 0, 0, 0},
  {49,70,97, 0}, {3,6,13,22}, {2,3,12,13}, {2,8,10, 0}, {59,84,89,96}, {18,33,48, 0},
  {6,7,0,0}, {3, 0,0,0}, {1,10,17,28}, {3,4,7,12}, {36,47,58,85}, {1,2,9,23}, {2,5,9, 0},
  {3,0,0,0}, {15,38,73,0}, {36,71,0,0}, {4,5,8,12}, {3,4,12,14}, {2,3,4,6}, {48,67,74,87}}';
	range1 int[] := '{64,71,76,81}';
	range2 int[] := '{92,135,264}';
	range3 int[] := '{20}';
	range4 int[] := '{49,70,97}';
	range5 int[] := '{3,6,13,22}';
	range6 int[] := '{2,3,12,13}';
	range7 int[] := '{2,8,10}';
	range8 int[] := '{59,84,89,96}';
	range9 int[] := '{18,33,48}';
	range10 int[] := '{6,7}';
	range11 int[] := '{3}';
	range12 int[] := '{1,10,17,28}';
	range13 int[] := '{3,4,7,12}';
	range14 int[] := '{36,47,58,85}';
	range15 int[] := '{1,2,9,23}';
	range16 int[] := '{2,5,9}';
	range17 int[] := '{3}';
	range18 int[] := '{15,38,73}';
	range19 int[] := '{36,71}';
	range20 int[] := '{4,5,8,12}';
	range21 int[] := '{3,4,12,14}';
	range22 int[] := '{2,3,4,6}';
	range23 int[] := '{48,67,74,87}';
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
	-- calculate all index of sublayers
  index_1:= feature_to_vector1(f1,range1, demisions[1]);

	index_2[1] = feature_to_vector1(f2,range2, demisions[2]);
	index_2[2] = feature_to_vector1(f3,range3, demisions[3]);
	index_2[3] = feature_to_vector1(f4,range4, demisions[4]);

	index_3:= feature_to_vector1(f5,range5, demisions[5]);

	index_4[1] = feature_to_vector1(f6,range6, demisions[6]);
	index_4[2] = feature_to_vector1(f7,range7, demisions[7]);
	index_4[3] = feature_to_vector1(f12,range12, demisions[12]);
	index_4[4] = feature_to_vector1(f13,range13, demisions[13]);

	index_5[1] = feature_to_vector1(f8,range8, demisions[8]);
	index_5[2] = feature_to_vector1(f9,range9, demisions[9]);
	index_5[3] = feature_to_vector1(f10,range10, demisions[10]);
	index_5[4] = feature_to_vector1(f11,range11, demisions[11]);

	index_6[1] = feature_to_vector1(f14,range14, demisions[14]);
	index_6[2] = feature_to_vector1(f19,range19, demisions[19]);
	index_6[3] = feature_to_vector1(f21,range21, demisions[21]);

	index_7[1] = feature_to_vector1(f15,range15, demisions[15]);
	index_7[2] = feature_to_vector1(f16,range16, demisions[16]);
	index_7[3] = feature_to_vector1(f17,range17, demisions[17]);

	index_8[1] = feature_to_vector1(f18,range18, demisions[18]);
	index_8[2] = feature_to_vector1(f20,range20, demisions[20]);

  index_9:= feature_to_vector1(f22,range22, demisions[22]);
  index_10:= feature_to_vector1(f23,range23, demisions[23]);
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
