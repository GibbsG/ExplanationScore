
-- get the index where the bucket is 1
create OR REPLACE function feature_to_vector1(
	value int,
  feature_range int[],
  feature_dim int
)
returns int as $$
DECLARE
  index int :=1;
  count int  := -1;
begin

  IF (value < 0) then
    IF (value = -7) then
      count = feature_dim+2;
    ELSIF (value = -8) then
      count = feature_dim+3;
    ELSIF (value = -9) then
      count = feature_dim+4;
    END IF;
  ELSE
    LOOP
      EXIT WHEN index = feature_dim+1;
      IF (value < feature_range[index]) THEN
        count := index;
        index := feature_dim;
      END IF;
      index := index+1;
    END LOOP;
    IF (count = -1) THEN
      count := feature_dim + 1;
    END IF;
  END IF;
  return count;
end;
$$
language plpgsql;
