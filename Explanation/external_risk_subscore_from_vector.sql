
-- get the subscore for external_risk_subscore_from_index
create function external_risk_subscore_from_index(
	index int
)
returns float as $$
DECLARE
  weights float[] := '{2.9895622, 2.1651128, 1.4081029, 0.7686735, 0, 0, 0, 1.6943381}';
  score_1 float := -1.4308699;
  raw float := 0;
begin
  raw :=  score_1+weights[index];
  raw := 1.5671672 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;



create function trade_open_time_subscore_from_index(
	index int[]
)
returns float as $$
DECLARE
  weights1 float[] := '{8.20842027e-01, 0.525120503, 0.245257364, 0.005524848, 0, 0.418318111, 0.435851213}';
  weights2 float[] := '{0.031074792, 0.006016629, 0, 0, 0.027688067}';
  weights3 float[] := '{1.209930852, 0.694452470, 0.296029824, 0, 0, 0, 0.471490736}';
  weight float := -0.696619002;
  raw float := 0;
begin
  raw := weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);
  raw := 2.5236825 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;


create OR REPLACE function num_sat_trades_subscore_from_index(
  index int
)
returns float as $$
DECLARE
  weights float[] := '{2.412574, 1.245278, 0.6619963, 0.2731984, 5.444148e-09, 0, 0, 0.4338848}';
  score_1 float := -0.1954726;
  raw float := 0;
begin
  raw :=  score_1+weights[index];
  raw := 2.1711503 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;



create OR REPLACE function trade_freq_subscore_from_index(
	index int[]
)
returns float as $$
DECLARE
  weights1 float[] := '{2.710260e-04, 9.195886e-01, 9.758620e-01, 1.008107e+01, 9.360290, 0, 0, 3.970360e-01}';
  weights2 float[] := '{1.514937e-01, 3.139667e-01, 0, 2.422345e-01, 0, 0, 3.095043e-02}';
  weights3 float[] := '{2.888436e-01, 9.659472e-01, 5.142479e-01, 2.653203e-01, 8.198233e-07, 0, 0, 3.233593e-01}';
  weights4 float[] := '{8.405069e-06, 3.374686e-01, 4.934466e-01, 8.601860e-01, 9.451724, 0, 0, 1.351433e-01}';
  weight float := -6.480598e-01;
  raw float := 0;
begin
  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]] + weights4[index[4]]);
  raw :=  0.3323177 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;



create OR REPLACE function delinquency_subscore_from_index(
	index int[]
)
returns float as $$
DECLARE
  weights1 float[] := '{1.658975, 1.218405, 8.030501e-01, 5.685712e-01, 0, 0, 0, 6.645698e-01}';
  weights2 float[] := '{4.014945e-01, 2.912651e-01, 5.665418e-02, 0,6.935965e-01, 5.470874e-01, 4.786956e-01}';
  weights3 float[] := '{1.004642, 5.654694e-01, 0, 0, 0, 2.841047e-01}';
  weights4 float[] := '{1.378803e-01, 1.101649e-06, 0, 0, 1.051132e-02}';
  weight float := -1.199469;
  raw float := 0;
begin
  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]] + weights4[index[4]]);
  raw := 2.5396631 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;


create OR REPLACE function installment_subscore_from_index(
	index int[]
)
returns float as $$
DECLARE
  raw float := 0;
  weights1 float[] := '{9.059412e-05, 1.292266e-01, 4.680034e-01, 8.117938e-01, 1.954441, 0, 0, 1.281830}';
  weights2 float[] := '{0, 1.432068e-01, 3.705526e-01, 0, 4.972869e-03, 1.513885e-01}';
  weights3 float[] := '{1.489759, 1.478176, 1.518328, 0, 9.585058e-01, 0, 1.506442, 5.561296e-01}';
  weight float := -1.750937;
begin
  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);
  raw := 0.9148520 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;




create OR REPLACE function inquiry_subscore_from_inedx(
	index int[]
)
returns float as $$
DECLARE
  weights1 float[] := '{1.907737, 1.260966, 1.010585, 8.318137e-01, 0, 1.951357, 0, 1.719356}';
  weights2 float[] := '{2.413596e-05, 2.251582e-01, 5.400251e-01, 1.255076, 0, 0, 1.061504e-01}';
  weights3 float[] := '{0, 6.095516e-02, 0, 0, 1.125418e-02}';
  weight float := -1.598351;
  raw float := 0;
begin
  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);
  raw := 3.0015073 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;



create OR REPLACE function revol_balance_subscore_from_index(
	index int[]
)
returns float as $$
DECLARE
  weights1 float[] := '{0.0001042232, 0.6764476961, 1.3938464180, 2.2581926077, 0, 1.7708134303, 1.0411847907}';
  weights2 float[] := '{ 0.0756555085, 0, 0.1175915408, 0.2823307493, 0.4242649887, 0, 0.8756715032, 0.0897134843}';
  weight float := -0.8924856930;
  raw float := 0;
begin
  raw :=  weight+(weights1[index[1]] + weights2[index[2]]);
  raw := 1.9259728 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;



create OR REPLACE function utilization_subscore_from_index(
	index int
)
returns float as $$
DECLARE
  weights float[] := '{0, 0.8562096, 1.2047649, 1.1635459, 1.4701220, 0, 1.2392294, 0.4800086}';
  score_1 float := -0.2415871;
  raw float := 0;
begin
  raw :=  score_1+weights[index];
  raw := 0.9864329 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;


create OR REPLACE function trade_w_balance_subscore_from_index(
	index int
)
returns float as $$
DECLARE
  weights float[] := '{0, 0.5966752, 0.9207121, 1.2749998, 1.8474869, 0, 2.2885183, 1.0606029}';
  score_1 float := -0.8221922;
  raw float := 0;
begin
  raw :=  score_1+weights[index];
  raw := 0.2949793 / (1+exp(-raw));
  return raw;
end;
$$
language plpgsql;






























--
