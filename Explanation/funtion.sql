create OR REPLACE function function0(
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
	f23 int,
	f24 int
)
returns int as $$
DECLARE
  score int := 0;
  subscore1 float := -1.4308699;
begin
		IF (f1 < 64) then
			subscore1 := 2.9895622 * subscore1;
    ELSIF (f1 < 71) then
      subscore1 := 2.1651128 * subscore1;
    ELSIF (f1 < 76) then
      subscore1 := 1.4081029 * subscore1;
    ELSIF (f1 < 81) then
      subscore1 := 0.7686735 * subscore1;
    ELSIF (f1 = -9) then
      subscore1 := 1.6943381 * subscore1;
	else
			subscore1:= 0;
	end if;
  subscore1 =
  return score;
end;
$$
language plpgsql;
