import javax.swing.*;
import java.io.*;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
public class SHAPScore {
    // all of the features' names in an array
    public static final String[] TEST_FEATURES = new String[]{"ExternalRiskEstimate", "MSinceOldestTradeOpen",
            "MSinceMostRecentTradeOpen",
            "AverageMInFile", "NumSatisfactoryTrades", "NumTrades60Ever2DerogPubRec", "NumTrades90Ever2DerogPubRec",
            "PercentTradesNeverDelq", "MSinceMostRecentDelq", "MaxDelq2PublicRecLast12M", "MaxDelqEver",
            "NumTotalTrades", "NumTradesOpeninLast12M", "PercentInstallTrades", "MSinceMostRecentInqexcl7days",
            "NumInqLast6M", "NumInqLast6Mexcl7days", "NetFractionRevolvingBurden", "NetFractionInstallBurden",
            "NumRevolvingTradesWBalance", "NumInstallTradesWBalance", "NumBank2NatlTradesWHighUtilization",
            "PercentTradesWBalance"
    };
    // the start time, helps to calculate the time used for this implementation
    public static Instant START;

    // run the program to get all the resp scores: it has two versions one using the SQL functions to calculate
    // the values of all subsets, the other using java to calculate the value. The SQL function has a slightly
    // better performance than the other
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn = connect();
        conn.setAutoCommit(false);


        // sample the test data and training data
        int length = 10459; // get the size of the test data

        int seed = 233; //create a seed for better use
        Random rand = new Random(234);
        int trainSize = 7896;

        START = Instant.now();
        //getSHAPScore(conn);
        getSHAPScore2(conn);
        conn.close();
    }

    /**
     * The SHAP score implementation using embedded sql function.
     *
     *
     * @param conn the connection to the database
     */
    public static void getSHAPScore(Connection conn) throws SQLException, IOException {
        createPermutationFunction(conn); // create the embedded SQL function
        // the path to the output file
        String outputPath = "/Users/gzx/Desktop/research/Explanation/src/output/SHAPScoreResult.txt";
        BufferedWriter outputStream = new BufferedWriter(new FileWriter(outputPath));
        // create the corresponding tables used in calculation
        createTableWithLabel(conn, "testDataWithLabel");
        createTableWithLabel(conn, "trainDataWithLabel");
        createStoreTabel(conn); // the table to store all the values of subset for given test entities
        insertTrainDataWithLabel(conn, "testDataWithLabel");

        // the array to stored the count of most important features for all test entities
        double[] mostImportantFeature = new double[23];

        // the array to stored the count of most 4 important features for all test entities
        int[] most4ImportantFeature = new int[23];

        // count the number of entities tested
        int count = 0;

        // get all test entities
        ResultSet testEntities = getTestEntityWithLabel(conn);

        // all factorial from 0 to 23
        double[] facOf23 = factorialOf23();
        while (testEntities.next()) {
            // check whether this is an entity that need to be explained
            if (testEntities.getInt(24) == 1) {
                count++;

                // used for timing checking
                if (count % 100 == 0) {
                    Instant end = Instant.now();
                    Duration timeElapsed = Duration.between(START, end);
                    System.out.println(count);
                    System.out.println(timeElapsed);
                    System.out.println(Arrays.toString(mostImportantFeature));
                    System.out.println(Arrays.toString(most4ImportantFeature));
                }

                // to store the scores for this test entities
                double[] scores = new double[23];

                // clear the table used for stored all values of subsets
                String deletePieceSQL = "delete from valid_subsets;";
                PreparedStatement deletePiece = conn.prepareStatement(deletePieceSQL);
                deletePiece.execute();

                // get the values of all subsets from the given test entities
                String getValueSQL = "select getPermutation( '{ ";
                for (int i = 1; i <= TEST_FEATURES.length; i++) {
                    getValueSQL = getValueSQL + testEntities.getInt(i);
                    if (i != TEST_FEATURES.length) {
                        getValueSQL = getValueSQL + ", ";
                    }
                }
                getValueSQL = getValueSQL + "}','{FALSE, FALSE, FALSE, FALSE, FALSE," +
                        " FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE," +
                        " FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE}', 0); ";
                PreparedStatement getValue = conn.prepareStatement(getValueSQL);
                getValue.execute();
                conn.commit();
                String retrieveValueSQL = "select * from valid_subsets;";
                PreparedStatement retrieveValue = conn.prepareStatement(retrieveValueSQL);
                ResultSet valueSet = retrieveValue.executeQuery();

                // to retrieve the values to a map for latter calculation
                Map<Integer, Double> representationToValue = new HashMap<>();
                while (valueSet.next()) {
                    representationToValue.put(valueSet.getInt(1), valueSet.getDouble(2));
                }

                // calculate the SHAP score!!!
                for (int represent : representationToValue.keySet()) {
                    double valueWithoutFeature = representationToValue.get(represent);
                    if (valueWithoutFeature != 1 && valueWithoutFeature != 0) {
                        // get the size of the subset
                        int setSize = 0;
                        int representation = represent / 2;
                        while (representation != 0) {
                            if (representation % 2 == 1) {
                                // the set contains a feature at that position
                                setSize++;
                            }
                            representation = representation / 2;
                        }

                        // only consider it is the smaller subset, and find the corresponding large subset
                        for (int i = 0; i < TEST_FEATURES.length; i++) {
                            // check whether the feature in the set
                            if (((int) (represent / Math.pow(2, i + 1))) % 2 == 0) { // not in the set
                                // get the representation of the larger subset that contains the feature
                                int newRepresent = (int) (represent + Math.pow(2, i + 1));
                                double valueWithFeature = 0;
                                // if we have the value of the larger subset, use it. If not, find the subset of it
                                // that stops expanding --  has value either 1 or 0
                                if (representationToValue.containsKey(newRepresent)) {
                                    valueWithFeature = representationToValue.get(newRepresent);
                                } else {
                                    int index = 23;
                                    int repre = newRepresent;
                                    if ((newRepresent - Math.pow(2, index)) > 0) {
                                        repre = (int) (newRepresent - Math.pow(2, index));
                                    }
                                    while (index > 0 && (!representationToValue.containsKey(repre))) {
                                        index--;
                                        if ((repre - Math.pow(2, index)) > 0) {
                                            repre = (int) (repre - Math.pow(2, index));
                                        }
                                    }
                                    valueWithFeature = representationToValue.get(repre);
                                }
                                // update values for given feature
                                scores[i] += ((valueWithFeature - valueWithoutFeature) *
                                        facOf23[TEST_FEATURES.length - setSize - 1] * facOf23[setSize]
                                        / facOf23[TEST_FEATURES.length]);
                            }
                        }
                    }
                }
                // get the most important features
                getMostImportantFeature(scores, mostImportantFeature, most4ImportantFeature);
                outputStream.write(Arrays.toString(scores));
                outputStream.newLine();
            }
        }
        System.out.println(Arrays.toString(mostImportantFeature));
        System.out.println(Arrays.toString(most4ImportantFeature));
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(START, end);
        System.out.println(count);
        System.out.println(timeElapsed);
        testEntities.close();
        outputStream.write(Arrays.toString(mostImportantFeature));
        outputStream.newLine();
        outputStream.write(Arrays.toString(most4ImportantFeature));
        outputStream.flush();
        outputStream.close();
    }


    /**
     * the shap implementation using java+sql
     *
     * @param conn the connection to the database
     */
    public static void getSHAPScore2(Connection conn) throws SQLException, IOException {
        String outputPath = "/Users/gzx/Desktop/research/Explanation/src/output/SHAPScoreResult.txt";
        BufferedWriter outputStream = new BufferedWriter(new FileWriter(outputPath));
        createTableWithLabel(conn, "testDataWithLabel");
        createTableWithLabel(conn, "trainDataWithLabel");
        createEntityTable(conn);
        insertTrainDataWithLabel(conn, "testDataWithLabel");
        double[] mostImportantFeature = new double[23];
        int[] most4ImportantFeature = new int[23];
        int count = 0;
        ResultSet testEntities = getTestEntityWithLabel(conn);
        List<String> features = Arrays.asList(TEST_FEATURES);
        while (testEntities.next()) {
            if (testEntities.getInt(24) == 1) {
                count++;
                if (count % 100 == 0) {
                    Instant end = Instant.now();
                    Duration timeElapsed = Duration.between(START, end);
                    System.out.println(count);
                    System.out.println(timeElapsed);
                }
                insertTestData(conn, testEntities);
                Map<Set<String>, Double> values = new HashMap<>();
                Set<String> subset = new HashSet<>();
                String getValueSQL = "SELECT avg(label) FROM testDataWithLabel; ";
                PreparedStatement getValue = conn.prepareStatement(getValueSQL);
                ResultSet valueSet = getValue.executeQuery();
                valueSet.next();
                double value = valueSet.getDouble(1);
                values.put(subset, value);
                getPermutation(conn, testEntities, subset, 0, features, values);
                double[] facOf23 = factorialOf23();
                double[] scores = new double[23];
                for (int i = 0; i < TEST_FEATURES.length; i++) {
                    Set<String> featureSet = new HashSet<>();
                    featureSet.add(TEST_FEATURES[i]);
                    Set<String> emptySet = new HashSet<>();
                    double score = 0;
                    if (values.containsKey(featureSet)) {
                        score = (values.get(featureSet) - values.get(emptySet)) / 23;
                    } else {
                        score = (0 - values.get(emptySet)) / 23;
                    }

                    for (int j = 1; j < TEST_FEATURES.length - 1; j++) {
                        double subScore = 0;
                        for (Set<String> sub : values.keySet()) {
                            if (sub.size() == j && !sub.contains(TEST_FEATURES[i])) {
                                Set<String> subNew = new HashSet<>(sub);
                                if ((values.get(subNew) != 1) && values.get(subNew) != 0) {
                                    subScore -= values.get(subNew);
                                    subNew.add(TEST_FEATURES[i]);
                                    int end = 22;
                                    while (!values.containsKey(subNew)) {
                                        while (end >= 0 && (!subNew.contains(TEST_FEATURES[end]))) {
                                            end--;
                                        }
                                        subNew.remove(TEST_FEATURES[end]);
                                    }
                                    subScore += values.get(subNew);
                                }
                            }
                        }
                        score += (subScore * facOf23[TEST_FEATURES.length - j - 1] * facOf23[j]
                                / facOf23[TEST_FEATURES.length]);
                    }
                    scores[i] = score;
                }
                outputStream.write(Arrays.toString(scores));
                outputStream.newLine();
                getMostImportantFeature(scores, mostImportantFeature, most4ImportantFeature);
            }
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(START, end);
        System.out.println(count);
        System.out.println(timeElapsed);
        System.out.println(Arrays.toString(mostImportantFeature));
        System.out.println(Arrays.toString(most4ImportantFeature));
        testEntities.close();
        outputStream.write(Arrays.toString(mostImportantFeature));
        outputStream.newLine();
        outputStream.write(Arrays.toString(most4ImportantFeature));
        outputStream.flush();
        outputStream.close();
    }

    /**
     * The the java method to get the values (the average label) of all subsets for a given test entities.
     * The result of all values is stored map from the subset to its value. Since the feature number is large,
     * it is really inefficient to calculate the value of all features.
     * One feature about the SHAP score is that if the value of the subset is 1 or 0, all the values of its superset
     * should be also corresponding 1 or 0. Hence, we can reduce the number of subset by using the deep-first approach
     * to visit all the features and stopping expanding when facing a subset has value 0 or 1.
     *
     * We start with subset of null and gradually adding features to the subsets using DFS and recursion.
     *
     * @param conn the connection to the database
     * @param testEntities the given test entities
     * @param set the current subset to considered
     * @param index the index whether next feature should start considering (the features before already considered and
     *              only add those latter to the subset.
     * @param features All the features
     * @param values the map to store the values of the subsets
     * @throws SQLException
     */
    public static void getPermutation(Connection conn, ResultSet testEntities, Set<String> set, int index, List<String> features,
                                      Map<Set<String>, Double> values) throws SQLException {
        for (int i = index; i < features.size(); i++) {
            Set<String> newSet = new HashSet<>(set);
            newSet.add(features.get(i));

            String getValueSQL = "SELECT avg(label) FROM testDataWithLabel where ";
            for (String feature : newSet) {
                getValueSQL = getValueSQL + feature + " = " +
                        testEntities.getInt(features.indexOf(feature) + 1) + " And ";
            }
            getValueSQL = getValueSQL.substring(0, getValueSQL.length() - 4) + ";";
            PreparedStatement getValue = conn.prepareStatement(getValueSQL);
            ResultSet valueSet = getValue.executeQuery();
            valueSet.next();

            double value = valueSet.getDouble(1);
            // continue to check all the super sets with valid value
            if (value != 0) {
                values.put(newSet, value);
                if (value != 1) {
                    getPermutation(conn, testEntities, newSet, i + 1, features, values);
                }
            }
        }
    }

    /**
     * compute the factorial of integers from 0 to 23 inclusive and return an array represent all factorials
     *
     * @return an array represent all 24 factorials
     */
    public static double[] factorialOf23() {
        double[] fac = new double[24];
        fac[0] = 1;
        fac[1] = 1;
        for (int i = 2; i <= 23; i++) {
            fac[i] = fac[i - 1] * i;
        }
        return fac;
    }

    /**
     * create the required table for the dataset -- this is specific with 24 column,
     * with 23 feature and extra one for the label for that feature (at index 24)
     *
     * @param conn the connection
     * @param tableName the name of the table to be created
     */
    public static void createTableWithLabel(Connection conn, String tableName) throws SQLException {
        String createTableSQL = "CREATE TABLE " + tableName + " ( ";
        for (int i = 0; i < TEST_FEATURES.length; i++) {
            createTableSQL = createTableSQL + TEST_FEATURES[i] + " int, ";
        }
        createTableSQL = createTableSQL + "label int );";
        PreparedStatement createTable = conn.prepareStatement(createTableSQL);
        createTable.execute();
        conn.commit();
    }

    /**
     * insert the training data with label from the specific file
     *
     * @param conn the connection to the database
     * @param testTableName the table in which data to be inserted
     */
    public static void insertTrainDataWithLabel(Connection conn, String testTableName) throws SQLException {
        BufferedReader test = null;
        BufferedReader train = null;
        String insertTestSQL = null;
        try {
            test = new BufferedReader(new FileReader("/Users/gzx/Documents/GitHub/explanation-scores/fico/testLabel.csv"));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }

        try {
            insertTestSQL = "INSERT INTO " + testTableName + "  VALUES ( ?";

            for (int i = 0; i < 23; i++) {
                insertTestSQL = insertTestSQL + ", ?";
            }
            String line = null;
            insertTestSQL = insertTestSQL + ");";
            PreparedStatement insertTestValue = conn.prepareStatement(insertTestSQL);
            while ((line = test.readLine()) != null) {
                String[] data = line.split(",");
                if (Integer.parseInt(data[1]) != -9 && Integer.parseInt(data[3]) != -9) { // remove the missing data
                    for (int i = 0; i < 24; i++) {
                        insertTestValue.setInt(i + 1, Integer.parseInt(data[i]));
                    }
                    insertTestValue.execute();
                }
            }
            conn.commit();
        } catch (SQLException | IOException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * get the test entity from the test data table -- the table is named "testDataWithBucket"
     *
     * @param conn the connection to the database
     * @return the test entities through resultSet
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static ResultSet getTestEntityWithLabel(Connection conn) throws SQLException {
        String getEntitySql = "Select * from testDataWithLabel;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySql);
        ResultSet result = getEntity.executeQuery();
        return result;
    }

    /**
     * get the most important and most 4 important feature from the test and store the result in the array passed
     * through parameter
     *
     * @param scores                the resp-score of that entity
     * @param mostImportantFeature  the array to store the most important feature of each entity
     * @param most4ImportantFeature the array to store the most 4 important feature of each entity
     */
    public static void getMostImportantFeature(double[] scores, double[] mostImportantFeature,
                                               int[] most4ImportantFeature) {
        int importantNumber = 4; // the number of important features to count
        int count = 0; // the number of valid non-zero features
        double[][] importantFeatures = new double[][]{{-1, Integer.MIN_VALUE}, {-1, Integer.MIN_VALUE},
                {-1, Integer.MIN_VALUE}, {-1, Integer.MIN_VALUE}};
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] == 0) {
                count++; // check whether still all zeros
            } else {
                // find the corresponding importance of the feature and update correspondingly
                for (int j = 0; j < importantNumber; j++) {
                    if (scores[i] > importantFeatures[j][1]) {
                        for (int k = importantNumber - 1; k > j; k--) {
                            importantFeatures[k][0] = importantFeatures[k - 1][0];
                            importantFeatures[k][1] = importantFeatures[k - 1][1];
                        }
                        importantFeatures[j][0] = i;
                        importantFeatures[j][1] = scores[i];
                        break;
                    }
                }
            }
        }
        // check whether is enough valid features
        if (count != 23) {
            // check for two features that are same most importance and update the most important feature
            for (int j = importantNumber - 1; j >= 0; j--) {
                if (importantFeatures[0][1] == importantFeatures[j][1]) {
                    for (int k = 0; k <= j; k++) {
                        mostImportantFeature[(int) importantFeatures[k][0]] += 1.0 / (j + 1);
                    }
                    break;
                }
            }

            //update the most important 4 features
            for (int i = 0; i < importantNumber; i++) {
                if (importantFeatures[i][0] != -1) {
                    most4ImportantFeature[(int) importantFeatures[i][0]]++;
                }
            }
        }


    }

    /**
     *  delete the original test entity and insert the new test entity into the test table piece -- this aims to use the
     *  sql joins to speed up.
     *
     * @param conn the connection to the database
     * @param testEntitySet the test piece
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static void insertTestData(Connection conn, ResultSet testEntitySet)
            throws SQLException {
        // to make sure that the table is empty before
        String deletePieceSQL = "delete from piece;";
        PreparedStatement deletePiece = conn.prepareStatement(deletePieceSQL);
        deletePiece.execute();
        // insert the test piece into the table to test
        String insertPieceSQL = "INSERT INTO piece VALUES (";
        for (int i = 1; i < TEST_FEATURES.length; i++) {
            insertPieceSQL  = insertPieceSQL + testEntitySet.getInt(i) + ", ";
        }
        insertPieceSQL = insertPieceSQL + testEntitySet.getInt(TEST_FEATURES.length) + ");";
        PreparedStatement insertPiece = conn.prepareStatement(insertPieceSQL);
        insertPiece.execute();
    }

    /**
     * create the table to score all the values of subsets for the test entity
     *
     * @param conn the connection to the database
     */
    public static void createStoreTabel(Connection conn) throws SQLException {
        String createSQL = "CREATE table valid_subsets(representation int, value float);";
        PreparedStatement create = conn.prepareStatement(createSQL);
        create.execute();
    }

    /**
     * create the test entity table
     *
     * @param conn the connection to the database
     * @return return the schema of the table
     * @throws IOException  this shouldn't if the file is valid
     * @throws SQLException this shouldn't happen if connect is correct with valid entity
     */
    public static String[] createEntityTable(Connection conn)
            throws IOException, SQLException {
        String path = "/Users/gzx/Desktop/research/fico/heloc_dataset_v2.csv";
        String tableName = "piece";
        BufferedReader reader = null;
        String[] names = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            names = reader.readLine().split(",");
            String createSQL = "CREATE TABLE " + tableName + " (" + names[1] + " int";
            for (int i = 2; i < names.length; i++) {
                createSQL = createSQL + ", "  + names[i] + " int";
            }
            createSQL = createSQL + ");";
            PreparedStatement createTable = conn.prepareStatement(createSQL);
            createTable.execute();
            conn.commit();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return names;
    }

    /**
     * The embedded SQL function to get the values (the average label) of all subsets for a given test entities.
     * The result of all values is stored in table "valid_subsets". It has two columns, one
     * is for the representation of that subset and the other is the value of that subset.
     * For the representation, it is like the binary (start at 2) for each feature according to the feature
     * index in the dataset. For example: the first feature "ExternalRiskEstimate" correspond to 2^1. If it is
     * in the subset, representation mod 2 is 1; otherwise 0. Similarly for the last feature "PercentTradesWBalance"
     * the value it corresponding to is 2^23. If it is in the subset, (representation/2^22)%2 == 1; otherwise 0.
     *
     * Also, since the feature number is large, it is really inefficient to calculate the value of all features.
     * One feature about the SHAP score is that if the value of the subset is 1 or 0, all the values of its superset
     * should be also corresponding 1 or 0. Hence, we can reduce the number of subset by using the deep-first approach
     * to visit all the features and stopping expanding when facing a subset has value 0 or 1.
     *
     * @param conn
     * @throws SQLException
     */
    public static void createPermutationFunction(Connection conn) throws SQLException {
        String functionSQL = "\n" +
                "create or replace function getPermutation(test_entity int[], features_set boolean[], start_index INTEGER)\n" +
                " returns INTEGER AS $$\n" +
                "DECLARE\n" +
                "   value_i float;\n" +
                "   representation int;\n" +
                "   feature_set_new boolean[23];\n" +
                "   i int := 0; -- the total amount of checks we used\n" +
                "BEGIN\n" +
                "  IF (start_index = 0) then\n" +
                "    SELECT into value_i avg(label)\n" +
                "    FROM testDataWithLabel;\n" +
                "\n" +
                "    INSERT INTO valid_subsets VALUES(0, value_i);\n" +
                "    start_index = start_index + 1;\n" +
                "  END IF;\n" +
                "\tLOOP\n" +
                "\t\tEXIT WHEN start_index = 24;\n" +
                "    i := i+1;\n" +
                "    feature_set_new = copy_array(features_set);\n" +
                "    -- get the new set of test_features\n" +
                "    feature_set_new[start_index] := TRUE;\n" +
                "    -- get the int represent the set\n" +
                "    representation := int_representation(feature_set_new);\n" +
                "    -- if not check, get the values\n" +
                "    value_i := get_value(feature_set_new, test_entity);\n" +
                "    -- if is a valid value, insert to the result table\n" +
                "    INSERT INTO valid_subsets VALUES(representation, value_i);\n" +
                "    If (value_i != 1 and value_i != 0) THEN\n" +
                "      -- check whether need to expand this branch as for deep first search\n" +
                "      i := i + getPermutation(test_entity, feature_set_new, start_index+1);\n" +
                "    END IF;\n" +
                "    start_index = start_index+1;\n" +
                "  END LOOP;\n" +
                "  return i;\n" +
                "END;\n" +
                "$$\n" +
                "LANGUAGE plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "CREATE or REPLACE function copy_array(old_array boolean[])\n" +
                "  returns boolean[] AS $$\n" +
                "DECLARE\n" +
                "  new_array boolean[23];\n" +
                "  i int := 1;\n" +
                "BEGIN\n" +
                "  LOOP\n" +
                "    new_array[i] := old_array[i];\n" +
                "    i := i + 1;\n" +
                "    EXIT WHEN i = 24;\n" +
                "  END LOOP;\n" +
                "  RETURN new_array;\n" +
                "END;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "CREATE or REPLACE function int_representation(test_features boolean[])\n" +
                "  returns int AS $$\n" +
                "DECLARE\n" +
                "  new_array boolean[23];\n" +
                "  representation int := 0;\n" +
                "  i int := 1;\n" +
                "BEGIN\n" +
                "  LOOP\n" +
                "    IF (test_features[i])\n" +
                "      THEN representation = representation + 2^i;\n" +
                "    END IF;\n" +
                "    i := i + 1;\n" +
                "    EXIT WHEN i = 24;\n" +
                "  END LOOP;\n" +
                "  RETURN representation;\n" +
                "END;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "CREATE or REPLACE function get_value(test_features boolean[], test_value int[])\n" +
                "  returns float AS $$\n" +
                "DECLARE\n" +
                "  return_value float;\n" +
                "BEGIN\n" +
                "  SELECT into return_value avg(label)\n" +
                "  FROM testDataWithLabel\n" +
                "  WHERE (test_features[1] = false or ExternalRiskEstimate = test_value[1])\n" +
                "    and (test_features[2] = false or MSinceOldestTradeOpen = test_value[2])\n" +
                "    and (test_features[3] = false or MSinceMostRecentTradeOpen = test_value[3])\n" +
                "    and (test_features[4] = false or AverageMInFile = test_value[4])\n" +
                "    and (test_features[5] = false or NumSatisfactoryTrades = test_value[5])\n" +
                "    and (test_features[6] = false or NumTrades60Ever2DerogPubRec = test_value[6])\n" +
                "    and (test_features[7] = false or NumTrades90Ever2DerogPubRec = test_value[7])\n" +
                "    and (test_features[8] = false or PercentTradesNeverDelq = test_value[8])\n" +
                "    and (test_features[9] = false or MSinceMostRecentDelq = test_value[9])\n" +
                "    and (test_features[10] = false or MaxDelq2PublicRecLast12M = test_value[10])\n" +
                "    and (test_features[11] = false or MaxDelqEver = test_value[11])\n" +
                "    and (test_features[12] = false or NumTotalTrades = test_value[12])\n" +
                "    and (test_features[13] = false or NumTradesOpeninLast12M = test_value[13])\n" +
                "    and (test_features[14] = false or PercentInstallTrades = test_value[14])\n" +
                "    and (test_features[15] = false or MSinceMostRecentInqexcl7days = test_value[15])\n" +
                "    and (test_features[16] = false or NumInqLast6M = test_value[16])\n" +
                "    and (test_features[17] = false or NumInqLast6Mexcl7days = test_value[17])\n" +
                "    and (test_features[18] = false or NetFractionRevolvingBurden = test_value[18])\n" +
                "    and (test_features[19] = false or NetFractionInstallBurden = test_value[19])\n" +
                "    and (test_features[20] = false or NumRevolvingTradesWBalance = test_value[20])\n" +
                "    and (test_features[21] = false or NumInstallTradesWBalance = test_value[21])\n" +
                "    and (test_features[22] = false or NumBank2NatlTradesWHighUtilization = test_value[22])\n" +
                "    and (test_features[23] = false or PercentTradesWBalance = test_value[23])\n" +
                "  ;\n" +
                "  RETURN return_value;\n" +
                "END;\n" +
                "$$\n" +
                "language plpgsql;\n";
        PreparedStatement function = conn.prepareStatement(functionSQL);
        function.execute();
    }


    /**
     * connect to the database of postgresql
     *
     * @return the connection to the database
     */
    public static Connection connect() {
        String url = "jdbc:postgresql://localhost:5432/research";
        String user = "gzx";
        String password = "WAGZgzaw0830";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }
}

