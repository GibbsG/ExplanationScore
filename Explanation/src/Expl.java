import java.io.*;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Expl {
    // all of the features' names in an array
    public static final String[] TEST_FEATURES  = new String[]{"ExternalRiskEstimate", "MSinceOldestTradeOpen",
            "MSinceMostRecentTradeOpen",
            "AverageMInFile", "NumSatisfactoryTrades", "NumTrades60Ever2DerogPubRec", "NumTrades90Ever2DerogPubRec",
            "PercentTradesNeverDelq", "MSinceMostRecentDelq", "MaxDelq2PublicRecLast12M", "MaxDelqEver",
            "NumTotalTrades", "NumTradesOpeninLast12M", "PercentInstallTrades", "MSinceMostRecentInqexcl7days",
            "NumInqLast6M", "NumInqLast6Mexcl7days", "NetFractionRevolvingBurden", "NetFractionInstallBurden",
            "NumRevolvingTradesWBalance", "NumInstallTradesWBalance", "NumBank2NatlTradesWHighUtilization",
            "PercentTradesWBalance"
    };
    private static final int[] MONOTONE = new int[]{-1, -1, -1, -1, -1, 1, 0, -1, -1, -1, -1, 0, 1, 1, -1, 1,
            1, 1, 1, 0, 0, 1, 1};
    private static final int[] DIMENSIONS = new int[] {5, 4, 2, 4, 5, 5, 4, 5, 4, 3, 2, 5, 5, 5, 5, 4, 2, 4, 3, 5, 5, 5, 5};

    private static final int[] BEST_INDEX = new int[] {5, 4, 2, 4, 5, 1, 3, 5, 4, 3, 2, 5, 1, 1, 5, 1, 1, 1, 1, 2, 4, 1, 1};
    public static  Instant START;
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn =  connect();
        conn.setAutoCommit(false);
        // createClassifier(conn);


        // sample the test data and training data
        int length = 10459; // get the size of the test data

        int seed = 233; //create a seed for better use
        Random rand = new Random(234);


        //sampleDataWithBucket(conn, length, rand, filePath);
        int trainSize = 7896;

        START = Instant.now();

        getRespScore(conn, trainSize);
       //getSHAPScore(conn);
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
     * @param conn the connection to the database
     */
    public static void getSHAPScore2(Connection conn) throws SQLException {
//        createTableWithLabel(conn, "testDataWithLabel");
//        createTableWithLabel(conn, "trainDataWithLabel");
//        insertTrainDataWithLabel(conn, "testDataWithLabel");
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
                        score = (values.get(featureSet) - values.get(emptySet))/23;
                    } else {
                        score = (0 - values.get(emptySet))/23;
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
                                        while (end >=0 && (!subNew.contains(TEST_FEATURES[end]))) {
                                            end--;
                                        }
                                        subNew.remove(TEST_FEATURES[end]);
                                    }
                                    subScore += values.get(subNew);
                                }
                            }
                        }
                        score += (subScore * facOf23[TEST_FEATURES.length -j-1] * facOf23[j]
                                / facOf23[TEST_FEATURES.length]);
                    }
                    scores[i] = score;
                }
                System.out.println(Arrays.toString(scores));
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
                        testEntities.getInt(features.indexOf(feature)+1) + " And ";
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
                    getPermutation(conn, testEntities, newSet, i+1, features, values);
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
            fac[i] = fac[i-1]*i;
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
            insertTestSQL = "INSERT INTO "+ testTableName + "  VALUES ( ?";

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
                        insertTestValue.setInt(i+1, Integer.parseInt(data[i]));
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
     * the method to get the resp score. Due to the calculation limitation, the calculation ends at contingent size
     * of one. The probability space is the Product Space.
     *
     *  Further implementation details:
     *  The views of each feature is named ff+featureName, for example, "ffExternalRiskEstimate" for the feature
     *  ExternalRiskEstimate.
     *  The test table named "testDataWithBucket" and the train table named "trainDataWithBucket", the entity to
     *  be test is stored in a table named "pieces" to be used in calculations.
     *
     * Example SQL query for null contingency set:
     * SELECT sum(f.count)
     * FROM piece as t, ffExternalRiskEstimate as f
     * WHERE classifier( f.ExternalRiskEstimate, t.MSinceOldestTradeOpen,
     *      t.MSinceMostRecentTradeOpen, t.AverageMInFile, t.NumSatisfactoryTrades, t.NumTrades60Ever2DerogPubRec,
     *         t.NumTrades90Ever2DerogPubRec, t.PercentTradesNeverDelq, t.MSinceMostRecentDelq,
     *         t.MaxDelq2PublicRecLast12M, t.MaxDelqEver, t.NumTotalTrades, t.NumTradesOpeninLast12M,
     *         t.PercentInstallTrades, t.MSinceMostRecentInqexcl7days, t.NumInqLast6M, t.NumInqLast6Mexcl7days,
     *         t.NetFractionRevolvingBurden, t.NetFractionInstallBurden, t.NumRevolvingTradesWBalance,
     *         t.NumInstallTradesWBalance, t.NumBank2NatlTradesWHighUtilization, t.PercentTradesWBalance) = 0;
     * This is for test feature "ExternalRiskEstimate", t is the test entity, f is the view of all possible value of the
     *  test feature
     *
     * Example SQL query for contingency set with size 1:
     * With temp as( select sum(f.count) as s
     * FROM piece as t, ffExternalRiskEstimate as f, ffMSinceMostRecentTradeOpen as c
     * WHERE classifier( f.ExternalRiskEstimate, t.MSinceOldestTradeOpen, c.MSinceMostRecentTradeOpen, t.AverageMInFile,
     * t.NumSatisfactoryTrades, t.NumTrades60Ever2DerogPubRec, t.NumTrades90Ever2DerogPubRec, t.PercentTradesNeverDelq,
     * t.MSinceMostRecentDelq, t.MaxDelq2PublicRecLast12M, t.MaxDelqEver, t.NumTotalTrades, t.NumTradesOpeninLast12M,
     * t.PercentInstallTrades, t.MSinceMostRecentInqexcl7days, t.NumInqLast6M, t.NumInqLast6Mexcl7days,
     * t.NetFractionRevolvingBurden, t.NetFractionInstallBurden, t.NumRevolvingTradesWBalance,
     * t.NumInstallTradesWBalance, t.NumBank2NatlTradesWHighUtilization, t.PercentTradesWBalance )= 0
     * Group BY c.MSinceMostRecentTradeOpen)
     *
     * SELECT max(s)
     * FROM temp;
     * This is for test feature "ExternalRiskEstimate" and contingent feature "MSinceMostRecentTradeOpen"
     * t is the test entity, f is the view of all possible value of the test feature,
     * c is is the view of all possible value of the contingent feature.
     *
     * @param conn the connection to the database
     * @param trainSize the train data size used to calculate the score
     */
    public static void getRespScore(Connection conn, int trainSize)
            throws SQLException, IOException {
        String dropQuery = "";
        for (int i = 0; i < TEST_FEATURES.length; i++) {
            dropQuery = dropQuery + " drop view if exists ff" + TEST_FEATURES[i] + " ; ";
        }
        dropQuery = dropQuery + "drop TABLE if exists piece; drop TABLE if exists testDataWithBucket; drop TABLE if exists trainDataWithBucket;";
        PreparedStatement drop = conn.prepareStatement(dropQuery);
        drop.execute();
        String path = "/Users/gzx/Desktop/research/fico/heloc_dataset_v2.csv";
        String testTableName = "testDataWithBucket";
        String trainTableName = "trainDataWithBucket";
        createTable(conn, path, testTableName);
        createTable(conn, path, trainTableName);
        // create the view of each fearture.
        insertForFixTestAndTrainWithBucket(conn,  "testDataWithBucket",
                "trainDataWithBucket");
       createViewWithBucketSize(conn, TEST_FEATURES); // comment as already run

        // create the table to store the next test entity
        createTable(conn, path, "piece");

        // let the arrays to stores the count of most important result
        double[] mostImportantFeature = new double[23];
        int[] most4ImportantFeature = new int[23];


        // get all test entities
        ResultSet testEntitySet =  getTestEntityWithBUcket(conn);

        // check the explained and unexplaned entities(unable to label due to the contingency of size 1)
        int zeroNo = 0;
        int zero = 0;
        int zero2 = 0;

        // then for each of the entity with bad outcome, we find the result -- the most important 1/4 features explain
        // the label
        int total = 0;
        while (testEntitySet.next()) {
            insertTestData(conn, testEntitySet);
            //only check for label == 1
            if (getLabel(conn) == 1) {
                if (total % 100 == 0) {
                    Instant end = Instant.now();
                    Duration timeElapsed = Duration.between(START, end);
                    System.out.println(total);
                    System.out.println(timeElapsed);
                }
                total++;
                // the scores for each feature with the entities
                double[] scores = getScoreWithNullContingencySetWithMonotonicity(conn, trainSize);
                // check that whether need to start with a contingency set, count is the number of 0-scores we get
                // with null contingent set
                int count = 0;
                for (int i = 0; i < TEST_FEATURES.length; i++) {
                    if (scores[i] == 0) {
                        count++;
                    }
                }
                // get the score with contingency set of size 1
                if (count == 23) {
                    // get the score with contingency set of size 1
                    scores = getScoreWithContingencySetSize1WithMonotoneExtra(conn, trainSize);
                    // check for correctness
                    if (!Arrays.equals(scores, getScoreWithContingencySetSize1WithMonotone(conn, trainSize))) {
                        System.out.println(Arrays.toString(scores));
                        System.out.println(Arrays.toString(getScoreWithContingencySetSize1WithMonotone(conn, trainSize)));
                        assert  false;
                    }
                    int countC1 = 0;
                    for (int i = 0; i < TEST_FEATURES.length; i++) {
                        if (scores[i] == 0) {
                            countC1++;
                        }
                    }
                    if (countC1 == 23) {
                        scores = getScoreWithContingencySetSize2WithMonotoneExtra(conn, trainSize);
                        zero2++;
                        System.out.println(zero2);
                    }
                    zero++;
                    //System.out.println(1);
                } else {
                    zeroNo++;
                }
                getMostImportantFeature(scores, mostImportantFeature, most4ImportantFeature);
                //System.out.println(Arrays.toString(scores));
//                System.out.println(Arrays.toString(mostImportantFeature));
//                System.out.println(Arrays.toString(most4ImportantFeature));
                conn.commit();
            }
            // when two contingent set with size 1, there will be two feature has the same score?

        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(START, end);
        System.out.println(timeElapsed);
        System.out.println(zeroNo);
        System.out.println(zero);
        System.out.println(zero2);
        System.out.println(Arrays.toString(mostImportantFeature));
        System.out.println(Arrays.toString(most4ImportantFeature));

    }

    /**
     * get the test entity from the test data table -- the table is named "testDataWithBucket"
     *
     * @param conn the connection to the database
     * @return the test entities through resultSet
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static ResultSet getTestEntityWithBUcket(Connection conn) throws SQLException {
        String getEntitySql = "Select * from testDataWithBucket;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySql);
        ResultSet result = getEntity.executeQuery();
        return result;
    }

    /**
     * get the resp score with null contingency set for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithNullContingencySetWithMonotonicity(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        // for each of the feature get the score
        String getEntitySQL = "select * from piece;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySQL);
        ResultSet entity = getEntity.executeQuery();
        entity.next();
        for (int i = 0; i < TEST_FEATURES.length; i++) {
            String getScoreSQL = "select sum(f.count * (1-classifier( ";
            for (int j = 0; j < TEST_FEATURES.length-1; j++) {
                if (i == j){
                    getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[j] + ", ";
                } else {
                    getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[j] + ", ";
                }
            }
            if (i == 22) {
                getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22] + " )))";
            } else {
                getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[22] + " )))";
            }
            getScoreSQL = getScoreSQL + "from piece as t, ff" + TEST_FEATURES[i] +" as f ";
            if (MONOTONE[i] == -1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                getScoreSQL = getScoreSQL + "where f." + TEST_FEATURES[i]  + "> t." + TEST_FEATURES[i] + " or f." +
                        TEST_FEATURES[i] + " >" + DIMENSIONS[i] + ";";
            } else if (MONOTONE[i] == 1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                getScoreSQL = getScoreSQL + "where f." + TEST_FEATURES[i]  + "< t." + TEST_FEATURES[i] + " or f." +
                        TEST_FEATURES[i] + " > " + DIMENSIONS[i] + ";";
            } else {
                getScoreSQL = getScoreSQL + ";";
            }
            // Stem.out.println(getScoreSQL);
            PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
            ResultSet resultSet = getScore.executeQuery();
            resultSet.next();
            double score = resultSet.getInt(1);

            resultSet.close();
            // check whether the score is 0 and need recompute with contingency set size 1
            // the contingency set can be any feature not the current feature -- feature[i]
            score = score / trainSize;
            scores[i] = score;
        }
        return scores;
    }

    /**
     * get the resp score with null contingency set for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithNullContingencySet(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        // for each of the feature get the score
        for (int i = 0; i < TEST_FEATURES.length; i++) {
            String getScoreSQL = "select sum(f.count) \n" +
                    "from piece as t, ff" + TEST_FEATURES[i] +" as f " +
                    "where classifier( ";
            for (int j = 0; j < TEST_FEATURES.length-1; j++) {
                if (i == j){
                    getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[j] + ", ";
                } else {
                    getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[j] + ", ";
                }
            }
            if (i == 22) {
                getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22] + " )= 0;";
            } else {
                getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[22] + " )= 0;";
            }

            PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
            ResultSet resultSet = getScore.executeQuery();
            resultSet.next();
            double score = resultSet.getInt(1);

            resultSet.close();
            // check whether the score is 0 and need recompute with contingency set size 1
            // the contingency set can be any feature not the current feature -- feature[i]
            score = score / trainSize;
            scores[i] = score;
        }
        return scores;
    }

    /**
     * get the resp score with contingency set size 1 for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithContingencySetSize1WithMonotoneExtra(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        // get the current entity used for checking whether the value of the feature is in undefined range
        String getEntitySQL = "select * from piece;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySQL);
        ResultSet entity = getEntity.executeQuery();
        entity.next();
        for (int i = 0; i < TEST_FEATURES.length; i++) {

            double score = 0;
            // for each feature check each of the contingency feature
            for (int j = 0; j < TEST_FEATURES.length; j++) {
                if (i != j) {
                    double currentScore = 0;
                    // set the other value to the best value, while others remain as the version without extra
                    String getScoreSQL = "With temp as( select f.count as count_i, classifier( ";
                    for (int k = 0; k < TEST_FEATURES.length - 1; k++) {
                        if (k == i) {
                            getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[k] + ", ";
                        } else if (k == j) {
                            getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[k] + ", ";
                        } else {
                            getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[k] + ", ";
                        }
                    }
                    if (i == 22) {
                        getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22] + " )";
                    } else if (j == 22) {
                        getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[22] + " )";
                    } else {
                        getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[22] + " )";
                    }
                    getScoreSQL = getScoreSQL + " as class, c." + TEST_FEATURES[j] + " from piece as t, ff" + TEST_FEATURES[i] + " as f, ff" +
                            TEST_FEATURES[j] + " as c where c." + TEST_FEATURES[j] + " = " + BEST_INDEX[j];
                    // if i has monotonicity, use it
                    if (MONOTONE[i] == -1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                        getScoreSQL = getScoreSQL + " and (f." +  TEST_FEATURES[i]  + "> t." + TEST_FEATURES[i] +
                                " or f." + TEST_FEATURES[i] + " > " + DIMENSIONS[i] + "))";
                    } else if (MONOTONE[i] == 1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                        getScoreSQL = getScoreSQL + " and (f." +  TEST_FEATURES[i]  + "< t." + TEST_FEATURES[i] +
                                " or f." + TEST_FEATURES[i] + " > " + DIMENSIONS[i] + "))";
                    } else {
                        getScoreSQL = getScoreSQL + ")";
                    }
                    getScoreSQL = getScoreSQL + ", tempFirst as (select sum(temp.count_i*(1-temp.class)) as s from temp  " +
                            "group by  temp." + TEST_FEATURES[j] + ")  select max(s) from tempFirst ;";
                    PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
                    //System.out.println(getScoreSQL);
                    ResultSet resultSet = getScore.executeQuery();
                    resultSet.next();
                    currentScore = resultSet.getInt(1);
                    resultSet.close();
                    if (score < currentScore) {
                        score = currentScore;
                    }
                }
            }
            score = score / trainSize /2;
            scores[i] = score;
        }
        return scores;
    }




    /**
     * get the resp score with contingency set size 1 for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithContingencySetSize1WithMonotone(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        double[][] values = new double[23][23];
        String getEntitySQL = "select * from piece;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySQL);
        ResultSet entity = getEntity.executeQuery();
        entity.next();
        for (int i = 0; i < TEST_FEATURES.length; i++) {

            double score = 0;
            // for each feature check each of the contingency feature
            for (int j = 0; j < TEST_FEATURES.length; j++) {
                if (i != j) {
                    double currentScore = 0;
                    if (i > j) {
                        currentScore = values[j][i];
                    } else {
                        String getScoreSQL = "With temp as( select f.count as count_i, c.count as count_j, f." +
                                TEST_FEATURES[i] + ", c." +  TEST_FEATURES[j] + ", classifier( ";
                        for (int k = 0; k < TEST_FEATURES.length-1; k++) {
                            if (k == i){
                                getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[k] + ", ";
                            } else if (k == j) {
                                getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[k] + ", ";
                            } else {
                                getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[k] + ", ";
                            }
                        }
                        if (i == 22) {
                            getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22] + " )";
                        } else if (j == 22) {
                            getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[22] + " )";
                        } else {
                            getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[22] + " )";
                        }
                        getScoreSQL = getScoreSQL + " as class from piece as t, ff" + TEST_FEATURES[i] + " as f, ff" +
                                TEST_FEATURES[j] + " as c ";
                        int before = 0;
                        if (MONOTONE[i] == -1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                            before = 1;
                            getScoreSQL = getScoreSQL + "where (f." + TEST_FEATURES[i]  + "> t." + TEST_FEATURES[i] +
                                    " or f." + TEST_FEATURES[i] + " > " + DIMENSIONS[i] + ")";
                        } else if (MONOTONE[i] == 1 && entity.getInt(i+1) <= DIMENSIONS[i]) {
                            before = 1;
                            getScoreSQL = getScoreSQL + "where (f." + TEST_FEATURES[i]  + "< t." + TEST_FEATURES[i] +
                                    " or f." + TEST_FEATURES[i] + " > " + DIMENSIONS[i] + ")";
                        }

                        if (MONOTONE[j] == -1 && entity.getInt(j+1) <= DIMENSIONS[j]) {
                            if (before!= 0) {
                                getScoreSQL = getScoreSQL + " or (c." + TEST_FEATURES[j]  +
                                        "> t." + TEST_FEATURES[j] + " or c." + TEST_FEATURES[j] + " > " + DIMENSIONS[j] + "))";
                            } else {
                                getScoreSQL = getScoreSQL + " where (c." + TEST_FEATURES[j]  +
                                        "> t." + TEST_FEATURES[j] + " or c." + TEST_FEATURES[j] + " > " + DIMENSIONS[j] + "))";
                            }
                        } else if (MONOTONE[j] == 1 && entity.getInt(j+1) <= DIMENSIONS[j]) {
                            if (before!= 0) {
                                getScoreSQL = getScoreSQL + " or (c." + TEST_FEATURES[j]  +
                                        "> t." + TEST_FEATURES[j] + " or c." + TEST_FEATURES[j] + " > " + DIMENSIONS[j] + "))";
                            } else {
                                getScoreSQL = getScoreSQL + " where (c." + TEST_FEATURES[j]  + "< t." + TEST_FEATURES[j]
                                        + " or c." + TEST_FEATURES[j] + " > " + DIMENSIONS[j] + "))";
                            }
                        } else {
                            getScoreSQL = getScoreSQL + ")";
                        }

                        getScoreSQL = getScoreSQL + ", tempFirst as (select sum(temp.count_i*(1-temp.class)) as s from temp  " +
                                "group by  temp." + TEST_FEATURES[j] + ") , temSecond  as " +
                                "(select sum(temp.count_j*(1-temp.class))  as s from temp  group by  temp." + TEST_FEATURES[i] +
                                ") select max(s), 1 from tempFirst union select max(s), 2 from temSecond" +
                                ";";
                        //System.out.println(getScoreSQL);
                        PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
                        ResultSet resultSet = getScore.executeQuery();
                        resultSet.next();
                        if (resultSet.getInt(2) == 1) {
                            currentScore = resultSet.getInt(1);
                            double nextScore = resultSet.getInt(1);
                            if (resultSet.next()) {
                                nextScore = resultSet.getInt(1);
                            }
                            values[i][j] = nextScore;
                        } else {
                            double nextScore = resultSet.getInt(1);
                            resultSet.next();
                            currentScore = resultSet.getInt(1);
                            values[i][j] = nextScore;
                        }
                        resultSet.close();
                    }
                    if (score < currentScore) {
                        score = currentScore;
                    }
                }
            }
            score = score / trainSize /2;
            scores[i] = score;
        }
        return scores;
    }


    /**
     * get the resp score with contingency set size 1 for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithContingencySetSize1(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        double[][] values = new double[23][23];
        for (int i = 0; i < TEST_FEATURES.length; i++) {

            double score = 0;
            // for each feature check each of the contingency feature
            for (int j = 0; j < TEST_FEATURES.length; j++) {
                if (i != j) {
                    double currentScore = 0;
                    if (i > j) {
                        currentScore = values[j][i];
                    } else {
                        String getScoreSQL = "With temp as( select f.count as count_i, c.count as count_j, f." +
                                TEST_FEATURES[i] + ", c." +  TEST_FEATURES[j] +
                                " from piece as t, ff" + TEST_FEATURES[i] +" as f, ff" + TEST_FEATURES[j] + " as c " +
                                " where classifier( ";
                        for (int k = 0; k < TEST_FEATURES.length-1; k++) {
                            if (k == i){
                                getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[k] + ", ";
                            } else if (k == j) {
                                getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[k] + ", ";
                            } else {
                                getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[k] + ", ";
                            }
                        }
                        if (i == 22) {
                            getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22] + " )= 0)";
                        } else if (j == 22) {
                            getScoreSQL = getScoreSQL + "c." + TEST_FEATURES[22] + " )= 0)";
                        } else {
                            getScoreSQL = getScoreSQL + "t." + TEST_FEATURES[22] + " )= 0)";
                        }
                        getScoreSQL = getScoreSQL + ", tempFirst as (select sum(temp.count_i) as s from temp  " +
                                "group by  temp." + TEST_FEATURES[j] + ") , temSecond  as (select sum(temp.count_j)  as s from " +
                                "temp ";
                        getScoreSQL = getScoreSQL + " group by  temp." + TEST_FEATURES[i] +
                                ") select max(s), 1 from tempFirst union select max(s), 2 from temSecond" +
                                ";";
                        PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
                        ResultSet resultSet = getScore.executeQuery();
                        resultSet.next();
                        if (resultSet.getInt(2) == 1) {
                            currentScore = resultSet.getInt(1);
                            double nextScore = resultSet.getInt(1);
                            if (resultSet.next()) {
                                nextScore = resultSet.getInt(1);
                            }
                            values[i][j] = nextScore;
                        } else {
                            double nextScore = resultSet.getInt(1);
                            resultSet.next();
                            currentScore = resultSet.getInt(1);
                            values[i][j] = nextScore;
                        }
                        resultSet.close();
                    }
                    if (score < currentScore) {
                        score = currentScore;
                    }
                }
            }
            score = score / trainSize /2;
            scores[i] = score;
        }
        return scores;
    }


    /**
     *get the resp score with contingency set size 1 for an entity
     *
     * @param conn connection to database
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithContingencySetSize2WithMonotoneExtra(Connection conn, int trainSize)
            throws SQLException {
        double[] scores = new double[TEST_FEATURES.length];
        // get the current entity used for checking whether the value of the feature is in undefined range
        String getEntitySQL = "select * from piece;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySQL);
        ResultSet entity = getEntity.executeQuery();
        entity.next();
        Set<Set<Integer>> zeroValues = new HashSet<>();
        // x is the current interest feature while y and z are the contingent feature
        for (int x = 0; x < TEST_FEATURES.length; x++) {
            double score = 0;
            // for each feature check each of the contingency feature
            for (int y = 0; y < TEST_FEATURES.length; y++) {
                if (x != y) {
                    for (int z = 0; z < TEST_FEATURES.length; z++) {
                        if (z != x && z != y) {
                            Set<Integer> value = new HashSet<>();
                            value.add(x);
                            value.add(y);
                            value.add(z);
                            if (!zeroValues.contains(value)) {
                                double currentScore = 0;
                                // set the other value to the best value, while others remain as the version without extra
                                String getScoreSQL = "With temp as( select sum(f.count * (1-classifier(";
                                for (int k = 0; k < TEST_FEATURES.length - 1; k++) {
                                    if (k == x) {
                                        getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[k] + ", ";
                                    } else if (k == y) {
                                        getScoreSQL = getScoreSQL + "c1." + TEST_FEATURES[k] + ", ";
                                    } else if (k == z) {
                                        getScoreSQL = getScoreSQL + "c2." + TEST_FEATURES[k] + ", ";
                                    } else {
                                        getScoreSQL = getScoreSQL + "e." + TEST_FEATURES[k] + ", ";
                                    }
                                }
                                if (x == 22) {
                                    getScoreSQL = getScoreSQL + "f." + TEST_FEATURES[22];
                                } else if (y == 22) {
                                    getScoreSQL = getScoreSQL + "c1." + TEST_FEATURES[22];
                                }  else if (z == 22) {
                                    getScoreSQL = getScoreSQL + "c2." + TEST_FEATURES[22];
                                } else {
                                    getScoreSQL = getScoreSQL + "e." + TEST_FEATURES[22];
                                }
                                getScoreSQL = getScoreSQL + "))) as s from piece as e, ff" + TEST_FEATURES[x] +
                                        " as f, ff" + TEST_FEATURES[y] + " as c1, ff" + TEST_FEATURES[z] + " as c2 " +
                                        "where c1." + TEST_FEATURES[y] + " = " + BEST_INDEX[y] + " and c2." +
                                        TEST_FEATURES[z] + " = " + BEST_INDEX[z];

                                if (MONOTONE[x] == -1 && entity.getInt(x+1) <= DIMENSIONS[x]) {
                                    getScoreSQL = getScoreSQL + " and (f." +  TEST_FEATURES[x]  + "> e." + TEST_FEATURES[x] +
                                            " or f." + TEST_FEATURES[x] + " > " + DIMENSIONS[x] + ")";
                                } else if (MONOTONE[x] == 1 && entity.getInt(x+1) <= DIMENSIONS[x]) {
                                    getScoreSQL = getScoreSQL + " and (f." +  TEST_FEATURES[x]  + "< e." + TEST_FEATURES[x] +
                                            " or f." + TEST_FEATURES[x] + " > " + DIMENSIONS[x] + ")";
                                }
                                getScoreSQL = getScoreSQL + " group by c1." + TEST_FEATURES[y] + ", c2." + TEST_FEATURES[z]
                                        + ") select max(s) from temp;";
                                PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
                                //System.out.println(getScoreSQL);
                                ResultSet resultSet = getScore.executeQuery();
                                resultSet.next();
                                currentScore = resultSet.getInt(1);
                                resultSet.close();
                                if (currentScore == 0) {
                                    zeroValues.add(value);
                                } else if (score < currentScore) {
                                    score = currentScore;
                                }
                            }
                        }
                    }

                }
            }
            score = score / trainSize /3;
            scores[x] = score;
        }
        return scores;
    }

    /**
     *  delete the original test entity and insert the new test entity into the test table piece -- this aims to use the
     *  sql joins to speed up.
     *
     * @param conn the connection to the database
     * @param testEntitySet the test piece
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static void insertTestData(Connection conn,  ResultSet testEntitySet)
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
     * get the label of the test entity from the classifier. 1 meaning the entity is label bad and 0 for good
     *
     * @param conn the connection to the database
     * @return return thee label from the classifier
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static int getLabel(Connection conn) throws SQLException {
        String checkLabelSQL = "select classifier( t.ExternalRiskEstimate, t.MSinceOldestTradeOpen, t.MSinceMostRecentTradeOpen, t.AverageMInFile, t.NumSatisfactoryTrades, t.NumTrades60Ever2DerogPubRec, t.NumTrades90Ever2DerogPubRec, t.PercentTradesNeverDelq, t.MSinceMostRecentDelq, t.MaxDelq2PublicRecLast12M, t.MaxDelqEver, t.NumTotalTrades, t.NumTradesOpeninLast12M, t.PercentInstallTrades, t.MSinceMostRecentInqexcl7days, t.NumInqLast6M, t.NumInqLast6Mexcl7days, t.NetFractionRevolvingBurden, t.NetFractionInstallBurden, t.NumRevolvingTradesWBalance, t.NumInstallTradesWBalance, t.NumBank2NatlTradesWHighUtilization, t.PercentTradesWBalance ) \n" +
                "from piece as t;";
        PreparedStatement checkLabel = conn.prepareStatement(checkLabelSQL);
        ResultSet resultSetForLabel = checkLabel.executeQuery();
        resultSetForLabel.next();
        int label = resultSetForLabel.getInt(1);
        resultSetForLabel.close();
        conn.commit();
        return label;
    }


    /**
     *  get the most important and most 4 important feature from the test and store the result in the array passed
     *  through parameter
     *
     * @param scores the resp-score of that entity
     * @param mostImportantFeature the array to store the most important feature of each entity
     * @param most4ImportantFeature the array to store the most 4 important feature of each entity
     */
    public static void getMostImportantFeature(double[] scores, double[] mostImportantFeature,
                                               int[] most4ImportantFeature){
        int importantNumber = 4; // the number of important features to count
        int count = 0; // the number of valid non-zero features
        double[][] importantFeatures = new double[][] {{-1, Integer.MIN_VALUE}, {-1, Integer.MIN_VALUE},
                {-1, Integer.MIN_VALUE}, {-1, Integer.MIN_VALUE}};
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] == 0) {
                count++; // check whether still all zeros
            } else {
                // find the corresponding importance of the feature and update correspondingly
                for (int j = 0; j < importantNumber; j++) {
                    if  (scores[i] > importantFeatures[j][1]) {
                        for (int k = importantNumber - 1; k > j; k--) {
                            importantFeatures[k][0] = importantFeatures[k-1][0];
                            importantFeatures[k][1] = importantFeatures[k-1][1];
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
            for (int j = importantNumber-1; j >= 0; j--) {
                if (importantFeatures[0][1] == importantFeatures[j][1]) {
                    for (int k = 0; k <= j; k++) {
                        mostImportantFeature[(int) importantFeatures[k][0]] += 1.0/(j+1);
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
     * create the view of each feature, the view consist all distinct value of the features and the count of that
     *  value. The value of feature has already been bucketlize, so the value of a feature is the index of bucket
     *  it belongs to
     *  All the views are named ff + feature name
     *
     * @param conn the connection to the database
     * @param features the features to create view
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static void createViewWithBucketSize(Connection conn, String[] features) throws SQLException {
        // create view of each feature;
        for (int i = 0; i < features.length; i++) {
            String createViewSql = "CREATE VIEW " + "FF" + features[i] + " as select " + features[i] +
                    ", count(*) from " + "trainDataWithBucket group by " + features[i] + ";";
            PreparedStatement createView = conn.prepareStatement(createViewSql);
            createView.executeUpdate();
        }
        conn.commit();
    }

    /**
     * sample the whole data into test data and train data, bucket the data and insert into corresponding table created
     *  the value of each feature is the index of bucket it belongs to for that feature
     *  the name of the test data table is "testDataWithBucket" and the name of the train data table is
     *  "trainDataWithBucket"
     *
     * @param length the size of entire data set
     * @param rand the random to generate the random numbers
     * @param conn the connection to the database
     * @param filePath the path where the data is stored (a csv file)
     * @throws SQLException this should happen if connect is correct with valid entity
     * @throws IOException this should happen if path is correct with valid entity
     */
    public static void sampleDataWithBucket(Connection conn, int length, Random rand, String filePath)
            throws SQLException, IOException {
        Set<Integer> testData = new HashSet<>();
        while (testData.size() < length/5) {
            testData.add(rand.nextInt(length));
        }
        String testTableName = "testDataWithBucket";
        String trainTableName = "trainDataWithBucket";
        createTable(conn, filePath, testTableName);
        createTable(conn, filePath, trainTableName);
        insertForTestAndTrainWithBucket(conn, filePath, testTableName, trainTableName, testData);
    }


    /**
     * create the table with given name and a csv file for the schema
     *
     * @param conn the connection to the database
     * @param path the path where the data is stored (a csv file)
     * @param tableName the name of the table to created
     * @return return the schema of the table
     * @throws IOException  this shouldn't if the file is valid
     * @throws SQLException this shouldn't happen if connect is correct with valid entity
     */
    public static String[] createTable(Connection conn, String path, String tableName)
            throws IOException, SQLException {
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
     * insert the test and train data with bucket representation -- the value is the index(started from 1) of bucket
     * it belongs and only insert the feature columns, ignoring the first column that indicates whether
     * the entity is good or bad
     * For example, for the "ExternalRiskEstimate", it has the bucket range [64,71,76,81]. Therefore the value to index
     * is: [0, 64) for 1,  [64,71) for 2, [71, 76) for 3, [76, 81) for 4, [81. infinity) for 5, -7 for 6, -8 for 7,
     * -9 for 8
     *
     * @param conn conn the connection to the database
     * @param path the path where the data is stored
     * @param testTableName the test table name
     * @param trainTableName the train table name
     * @param testSet the set of index of data for test data
     * @throws SQLException  this should happen if connect is correct with valid entity
     */
    public static void insertForTestAndTrainWithBucket(Connection conn, String path, String testTableName,
                                                       String trainTableName, Set<Integer> testSet)
            throws SQLException {
        String insertTestSQL = null;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            // the bucket range of each feature
            int[][] featureRanges = new int[][] {{64, 71, 76, 81}, {92, 135, 264}, {20},
            {49, 70, 97}, {3, 6, 13, 22}, {2, 3, 12, 13}, {2, 8, 10}, {59, 84, 89, 96}, {18, 33, 48},
            {6, 7}, {3}, {1, 10, 17, 28}, {3, 4, 7, 12}, {36, 47, 58, 85}, {1, 2, 9, 23}, {2, 5, 9},
            {3}, {15, 38, 73}, {36, 71}, {4, 5, 8, 12}, {3, 4, 12, 14}, {2, 3, 4, 6}, {48, 67, 74, 87}};

            String line = null;
            int length =  reader.readLine().split(",").length;
            insertTestSQL = "INSERT INTO "+ testTableName + "  VALUES ( ?";
            String insertTrainSQL = "INSERT INTO "+ trainTableName + "  VALUES ( ?";

            for (int i = 1; i < length-1; i++) {
                insertTestSQL = insertTestSQL + ", ?";
                insertTrainSQL = insertTrainSQL + ", ?";
            }
            insertTestSQL = insertTestSQL + ");";
            insertTrainSQL = insertTrainSQL + ");";

            PreparedStatement insertTestValue = conn.prepareStatement(insertTestSQL);
            PreparedStatement insertTrainValue = conn.prepareStatement(insertTrainSQL);
            int index = 0;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                if (Integer.parseInt(data[1]) != -9 && Integer.parseInt(data[3]) != -9) { // remove the missing data
                    if (testSet.contains(index)) {
                        for (int i = 1; i < length; i++) {
                            insertTestValue.setInt(i, getIndexValue(data[i], featureRanges[i-1]));
                        }
                        insertTestValue.execute();
                    } else {
                        for (int i = 1; i < length; i++) {
                            insertTrainValue.setInt(i, getIndexValue(data[i], featureRanges[i-1]));
                        }
                        insertTrainValue.execute();
                    }
                }
                index++;
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
     * get the index value from a feature value for a given feature and given range
     * For example, for the "ExternalRiskEstimate", it has the bucket range [64,71,76,81]. Therefore the value to index
     * is: [0, 64) for 1,  [64,71) for 2, [71, 76) for 3, [76, 81) for 4, [81. infinity) for 5, -7 for 6, -8 for 7,
     * -9 for 8
     *
     * @param valueString  the value of the feature in String
     * @param featureRange the range of the feature
     * @return the index of the value given the feature
     */
    public static int getIndexValue(String valueString, int[] featureRange) {
        int value = Integer.parseInt(valueString);
        int indexOfValue = featureRange.length+1;
        if (value == -7) {
            indexOfValue = featureRange.length + 2;
        } else if (value == -8) {
            indexOfValue = featureRange.length + 3;
        } else if (value == -9) {
            indexOfValue = featureRange.length + 4;
        } else {
            for (int j = 0; j < featureRange.length; j++) {
                if (value < featureRange[j]) {
                    indexOfValue = j+1;
                    break;
                }
            }
        }
        return indexOfValue;
    }

    public static void insertForFixTestAndTrainWithBucket(Connection conn,  String testTableName,
                                                       String trainTableName)
            throws SQLException {
        String insertTestSQL = null;
        BufferedReader test = null;
        BufferedReader train = null;
        try {
            test = new BufferedReader(new FileReader("/Users/gzx/Documents/GitHub/ExplanationScore/Explanation/test.csv"));
            train = new BufferedReader(new FileReader("/Users/gzx/Documents/GitHub/ExplanationScore/Explanation/test.csv"));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            // the bucket range of each feature
            int[][] featureRanges = new int[][] {{64, 71, 76, 81}, {92, 135, 264}, {20},
                    {49, 70, 97}, {3, 6, 13, 22}, {2, 3, 12, 13}, {2, 8, 10}, {59, 84, 89, 96}, {18, 33, 48},
                    {6, 7}, {3}, {1, 10, 17, 28}, {3, 4, 7, 12}, {36, 47, 58, 85}, {1, 2, 9, 23}, {2, 5, 9},
                    {3}, {15, 38, 73}, {36, 71}, {4, 5, 8, 12}, {3, 4, 12, 14}, {2, 3, 4, 6}, {48, 67, 74, 87}};

            String line = null;
            int length =  23;
            insertTestSQL = "INSERT INTO "+ testTableName + "  VALUES ( ?";
            String insertTrainSQL = "INSERT INTO "+ trainTableName + "  VALUES ( ?";

            for (int i = 0; i < length-1; i++) {
                insertTestSQL = insertTestSQL + ", ?";
                insertTrainSQL = insertTrainSQL + ", ?";
            }
            insertTestSQL = insertTestSQL + ");";
            insertTrainSQL = insertTrainSQL + ");";

            PreparedStatement insertTestValue = conn.prepareStatement(insertTestSQL);
            PreparedStatement insertTrainValue = conn.prepareStatement(insertTrainSQL);
            while ((line = test.readLine()) != null) {
                String[] data = line.split(",");
                if (Integer.parseInt(data[1]) != -9 && Integer.parseInt(data[3]) != -9) { // remove the missing data
                    for (int i = 0; i < length; i++) {
                        insertTestValue.setInt(i+1, getIndexValue(data[i], featureRanges[i]));
                    }
                    insertTestValue.execute();
                }
            }
            while ((line = train.readLine()) != null) {
                String[] data = line.split(",");
                if (Integer.parseInt(data[1]) != -9 && Integer.parseInt(data[3]) != -9) { // remove the missing data
                    for (int i = 0; i < length; i++) {
                        insertTrainValue.setInt(i+1, getIndexValue(data[i], featureRanges[i]));
                    }
                    insertTrainValue.execute();
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
     * create the classifier in the database
     *
     * @param conn the connection to the database
     */
    public static void createClassifier(Connection conn) throws SQLException {
        String functionForFeatureSQL =
                "create function classifier(\n" +
                "\tf1 int,\n" +
                "\tf2 int,\n" +
                "\tf3 int,\n" +
                "\tf4 int,\n" +
                "\tf5 int,\n" +
                "\tf6 int,\n" +
                "\tf7 int,\n" +
                "\tf8 int,\n" +
                "\tf9 int,\n" +
                "\tf10 int,\n" +
                "\tf11 int,\n" +
                "\tf12 int,\n" +
                "\tf13 int,\n" +
                "\tf14 int,\n" +
                "\tf15 int,\n" +
                "\tf16 int,\n" +
                "\tf17 int,\n" +
                "\tf18 int,\n" +
                "\tf19 int,\n" +
                "\tf20 int,\n" +
                "\tf21 int,\n" +
                "\tf22 int,\n" +
                "\tf23 int\n" +
                ")\n" +
                "returns int as $$\n" +
                "DECLARE\n" +
                "    weights1 float[] := '{2.9895622, 2.1651128, 1.4081029, 0.7686735, 0, 0, 0, 1.6943381}';\n" +
                "\n" +
                "    weights2_1 float[] := '{8.20842027e-01, 0.525120503, 0.245257364, 0.005524848, 0, 0.418318111, 0.435851213}';\n" +
                "    weights2_2 float[] := '{0.031074792, 0.006016629, 0, 0, 0.027688067}';\n" +
                "    weights2_3 float[] := '{1.209930852, 0.694452470, 0.296029824, 0, 0, 0, 0.471490736}';\n" +
                "\n" +
                "    weights3 float[] := '{2.412574, 1.245278, 0.6619963, 0.2731984, 5.444148e-09, 0, 0, 0.4338848}';\n" +
                "\n" +
                "    weights4_1 float[] := '{2.710260e-04, 9.195886e-01, 9.758620e-01, 1.008107e+01, 9.360290, 0, 0, 3.970360e-01}';\n" +
                "    weights4_2 float[] := '{1.514937e-01, 3.139667e-01, 0, 2.422345e-01, 0, 0, 3.095043e-02}';\n" +
                "    weights4_3 float[] := '{2.888436e-01, 9.659472e-01, 5.142479e-01, 2.653203e-01, 8.198233e-07, 0, 0, 3.233593e-01}';\n" +
                "    weights4_4 float[] := '{8.405069e-06, 3.374686e-01, 4.934466e-01, 8.601860e-01, 9.451724, 0, 0, 1.351433e-01}';\n" +
                "\n" +
                "    weights5_1 float[] := '{1.658975, 1.218405, 8.030501e-01, 5.685712e-01, 0, 0, 0, 6.645698e-01}';\n" +
                "    weights5_2 float[] := '{4.014945e-01, 2.912651e-01, 5.665418e-02, 0,6.935965e-01, 5.470874e-01, 4.786956e-01}';\n" +
                "    weights5_3 float[] := '{1.004642, 5.654694e-01, 0, 0, 0, 2.841047e-01}';\n" +
                "    weights5_4 float[] := '{1.378803e-01, 1.101649e-06, 0, 0, 1.051132e-02}';\n" +
                "\n" +
                "    weights6_1 float[] := '{9.059412e-05, 1.292266e-01, 4.680034e-01, 8.117938e-01, 1.954441, 0, 0, 1.281830}';\n" +
                "    weights6_2 float[] := '{0, 1.432068e-01, 3.705526e-01, 0, 4.972869e-03, 1.513885e-01}';\n" +
                "    weights6_3 float[] := '{1.489759, 1.478176, 1.518328, 0, 9.585058e-01, 0, 1.506442, 5.561296e-01}';\n" +
                "\n" +
                "    weights7_1 float[] := '{1.907737, 1.260966, 1.010585, 8.318137e-01, 0, 1.951357, 0, 1.719356}';\n" +
                "    weights7_2 float[] := '{2.413596e-05, 2.251582e-01, 5.400251e-01, 1.255076, 0, 0, 1.061504e-01}';\n" +
                "    weights7_3 float[] := '{0, 6.095516e-02, 0, 0, 1.125418e-02}';\n" +
                "\n" +
                "    weights8_1 float[] := '{0.0001042232, 0.6764476961, 1.3938464180, 2.2581926077, 0, 1.7708134303, 1.0411847907}';\n" +
                "    weights8_2 float[] := '{ 0.0756555085, 0, 0.1175915408, 0.2823307493, 0.4242649887, 0, 0.8756715032, 0.0897134843}';\n" +
                "\n" +
                "    weights9 float[] := '{0, 0.8562096, 1.2047649, 1.1635459, 1.4701220, 0, 1.2392294, 0.4800086}';\n" +
                "\n" +
                "    weights10 float[] := '{0, 0.5966752, 0.9207121, 1.2749998, 1.8474869, 0, 2.2885183, 1.0606029}';\n" +
                "\n" +
                "score float := -8.3843046;\n" +
                "begin\n" +
                "\n" +
                "score := score + 1.5671672 / (1+exp(-(1.4308699 + weights1[f1]))) +\n" +
                "            2.5236825 / (1 + exp(-(-0.696619002 + weights2_1[f2] + weights2_2[f3] + weights2_3[f4])))\n" +
                "            + 2.1711503 / (1+exp(-(1.5671672+weights3[f5]))) +\n" +
                "            0.3323177 / (1+exp(-(-6.480598e-01+weights4_1[f6] + weights4_2[f7] + weights4_3[f12] +\n" +
                "            weights4_4[f13]))) +\n" +
                "            2.5396631 / (1+exp(-(-1.199469+weights5_1[f8] + weights5_2[f9] + weights5_3[f10] +\n" +
                "            weights5_4[f11]))) +\n" +
                "            0.9148520 / (1+exp(-(-1.750937+weights6_1[f14] + weights6_2[f19] + weights6_3[f21]))) +\n" +
                "            3.0015073 / (1+exp(-(-1.598351+(weights7_1[f15] + weights7_2[f16] + weights7_3[f17])))) +\n" +
                "            1.9259728 / (1+exp(-(-0.8924856930+weights8_1[f18] + weights8_2[f20]))) +\n" +
                "            0.9864329 / (1+exp(-(-0.2415871+weights9[f22]))) +\n" +
                "            0.2949793 / (1+exp(-(-0.8221922+weights10[f23])));\n" +
                "score:= 1 / (1+exp(-(score)));\n" +
                "IF (score < 0.5) THEN\n" +
                "\treturn 0;\n" +
                "ELSE\n" +
                "\treturn 1;\n" +
                "END IF;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n";
        PreparedStatement functionForFeature = conn.prepareStatement(functionForFeatureSQL);
        functionForFeature.execute();
        conn.commit();
    }
}
