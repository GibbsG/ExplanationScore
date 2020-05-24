
create or replace function getPermutation(test_entity int[], features_set boolean[], start_index INTEGER)
 returns INTEGER AS $$
DECLARE
   value_i float;
   representation int;
   feature_set_new boolean[23];
   i int := 0; -- the total amount of checks we used
BEGIN
  IF (start_index = 0) then
    SELECT into value_i avg(label)
    FROM testDataWithLabel;

    INSERT INTO valid_subsets VALUES(0, value_i);
    start_index = start_index + 1;
  END IF;
	LOOP
		EXIT WHEN start_index = 24;
    i := i+1;
    feature_set_new = copy_array(features_set);
    -- get the new set of test_features
    feature_set_new[start_index] := TRUE;
    -- get the int represent the set
    representation := int_representation(feature_set_new);
    -- if not check, get the values
    value_i := get_value(feature_set_new, test_entity);
    -- if is a valid value, insert to the result table
    INSERT INTO valid_subsets VALUES(representation, value_i);
    If (value_i != 1 and value_i != 0) THEN
      -- check whether need to expand this branch as for deep first search
      i := i + getPermutation(test_entity, feature_set_new, start_index+1);
    END IF;
    start_index = start_index+1;
  END LOOP;
  return i;
END;
$$
LANGUAGE plpgsql;




CREATE or REPLACE function copy_array(old_array boolean[])
  returns boolean[] AS $$
DECLARE
  new_array boolean[23];
  i int := 1;
BEGIN
  LOOP
    new_array[i] := old_array[i];
    i := i + 1;
    EXIT WHEN i = 24;
  END LOOP;
  RETURN new_array;
END;
$$
language plpgsql;


CREATE or REPLACE function int_representation(test_features boolean[])
  returns int AS $$
DECLARE
  new_array boolean[23];
  representation int := 0;
  i int := 1;
BEGIN
  LOOP
    IF (test_features[i])
      THEN representation = representation + 2^i;
    END IF;
    i := i + 1;
    EXIT WHEN i = 24;
  END LOOP;
  RETURN representation;
END;
$$
language plpgsql;



CREATE or REPLACE function get_value(test_features boolean[], test_value int[])
  returns float AS $$
DECLARE
  return_value float;
BEGIN
  SELECT into return_value avg(label)
  FROM testDataWithLabel
  WHERE (test_features[1] = false or ExternalRiskEstimate = test_value[1])
    and (test_features[2] = false or MSinceOldestTradeOpen = test_value[2])
    and (test_features[3] = false or MSinceMostRecentTradeOpen = test_value[3])
    and (test_features[4] = false or AverageMInFile = test_value[4])
    and (test_features[5] = false or NumSatisfactoryTrades = test_value[5])
    and (test_features[6] = false or NumTrades60Ever2DerogPubRec = test_value[6])
    and (test_features[7] = false or NumTrades90Ever2DerogPubRec = test_value[7])
    and (test_features[8] = false or PercentTradesNeverDelq = test_value[8])
    and (test_features[9] = false or MSinceMostRecentDelq = test_value[9])
    and (test_features[10] = false or MaxDelq2PublicRecLast12M = test_value[10])
    and (test_features[11] = false or MaxDelqEver = test_value[11])
    and (test_features[12] = false or NumTotalTrades = test_value[12])
    and (test_features[13] = false or NumTradesOpeninLast12M = test_value[13])
    and (test_features[14] = false or PercentInstallTrades = test_value[14])
    and (test_features[15] = false or MSinceMostRecentInqexcl7days = test_value[15])
    and (test_features[16] = false or NumInqLast6M = test_value[16])
    and (test_features[17] = false or NumInqLast6Mexcl7days = test_value[17])
    and (test_features[18] = false or NetFractionRevolvingBurden = test_value[18])
    and (test_features[19] = false or NetFractionInstallBurden = test_value[19])
    and (test_features[20] = false or NumRevolvingTradesWBalance = test_value[20])
    and (test_features[21] = false or NumInstallTradesWBalance = test_value[21])
    and (test_features[22] = false or NumBank2NatlTradesWHighUtilization = test_value[22])
    and (test_features[23] = false or PercentTradesWBalance = test_value[23])
  ;
  RETURN return_value;
END;
$$
language plpgsql;
