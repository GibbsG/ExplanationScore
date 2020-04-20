import java.io.*;
import java.sql.*;
import java.util.*;

public class Expl {
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn =  connect();
        conn.setAutoCommit(false);
        //createClassifier(conn);
        String filePath = "/Users/gzx/Desktop/research/fico/heloc_dataset_v2.csv";

        // the features to give the explanations
        String[] testFeatures  = new String[]{"ExternalRiskEstimate", "MSinceOldestTradeOpen",
                "MSinceMostRecentTradeOpen",
                "AverageMInFile", "NumSatisfactoryTrades", "NumTrades60Ever2DerogPubRec", "NumTrades90Ever2DerogPubRec",
                "PercentTradesNeverDelq", "MSinceMostRecentDelq", "MaxDelq2PublicRecLast12M", "MaxDelqEver",
                "NumTotalTrades", "NumTradesOpeninLast12M", "PercentInstallTrades", "MSinceMostRecentInqexcl7days",
                "NumInqLast6M", "NumInqLast6Mexcl7days", "NetFractionRevolvingBurden", "NetFractionInstallBurden",
                "NumRevolvingTradesWBalance", "NumInstallTradesWBalance", "NumBank2NatlTradesWHighUtilization",
                "PercentTradesWBalance"
        };

        // sample the test data and training data
        int length = 10459; // get the size of the test data

        int seed = 233; //create a seed for better use
        Random rand = new Random(234);
        sampleDataWithBucket(conn, length, rand, filePath);
        int trainSize = 7883;


        getRespScore(conn, testFeatures, trainSize, filePath);
        conn.close();
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
     * @param testFeatures names of all the test feature of any entity
     * @param trainSize the train data size used to calculate the score
     */
    public static void getRespScore(Connection conn, String[] testFeatures, int trainSize, String path)
            throws SQLException, IOException {
        // create the view of each fearture.
        createViewWithBucketSize(conn, testFeatures); // comment as already run

        // let the arrays to stores the count of most important result
        int[] mostImportantFeature = new int[23];
        int[] most4ImportantFeature = new int[23];
        createTable(conn, path, "piece");

        // get all test entities
        ResultSet testEntitySet =  getTestEntityWithBUcket(conn);

        // check the explained and unexplaned entities(unable to label due to the contingency of size 1)
        int zeroNo = 0;
        int zero = 0;
        int zero2 = 0;

        // then for each of the entity with bad outcome, we find the result -- the most important 1/4 features explain
        // the label
        while (testEntitySet.next()) {
            insertTestData(conn, testFeatures, testEntitySet);

            //only check for label == 1
            if (getLabel(conn) == 1) {

                // the scores for each feature with the entities
                double[] scores = getScoreWithNullContingencySet(conn, testFeatures, trainSize);

                // check that whether need to start with a contingency set, count is the number of 0-scores we get
                // with null contingent set
                int count = 0;
                for (int i = 0; i < testFeatures.length; i++) {
                    if (scores[i] == 0) {
                        count++;
                    }
                }
                // get the score with contingency set of size 1
                if (count == 23) {

                    // get the score with contingency set of size 1
                    scores = getScoreWithContingencySetSize1(conn, testFeatures, trainSize);

                    // check the number of 0s in scores
                    int zeroFor1 = 0;
                    for (int i = 0; i < testFeatures.length; i++) {
                        if (scores[i] == 0) {
                            zeroFor1++;
                        }
                    }
                    if (zeroFor1==23) {
                        zero2++;
                    }
                    zero++;
                } else {
                    zeroNo++;
                }
                getMostImportantFeature(scores, mostImportantFeature, most4ImportantFeature);
                conn.commit();
                System.out.println(Arrays.toString(scores));
            }
            // when two contingent set with size 1, there will be two feature has the same score?

        }
        System.out.println(zeroNo);
        System.out.println(zero);
        System.out.println(zero2);
        System.out.println(Arrays.toString(mostImportantFeature));
        System.out.println(Arrays.toString(most4ImportantFeature));

    }

    /**
     * get the resp score with null contingency set for an entity
     *
     * @param conn connection to database
     * @param testFeatures names of all test features
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithNullContingencySet(Connection conn, String[] testFeatures, int trainSize)
            throws SQLException {
        double[] scores = new double[testFeatures.length];
        for (int i = 0; i < testFeatures.length; i++) {
            String getScoreSQL = "select sum(f.count) \n" +
                    "from piece as t, ff" + testFeatures[i] +" as f " +
                    "where classifier( ";
            for (int j = 0; j < testFeatures.length-1; j++) {
                if (i == j){
                    getScoreSQL = getScoreSQL + "f." + testFeatures[j] + ", ";
                } else {
                    getScoreSQL = getScoreSQL + "t." + testFeatures[j] + ", ";
                }
            }
            if (i == 22) {
                getScoreSQL = getScoreSQL + "f." + testFeatures[22] + " )= 0;";
            } else {
                getScoreSQL = getScoreSQL + "t." + testFeatures[22] + " )= 0;";
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
     * @param testFeatures names of all test features
     * @param trainSize the train data size
     * @return the scores of each feature given the entity in the piece table
     */
    public static double[] getScoreWithContingencySetSize1(Connection conn, String[] testFeatures, int trainSize)
            throws SQLException {
        double[] scores = new double[testFeatures.length];
        for (int i = 0; i < testFeatures.length; i++) {
            double score = 0;
            // for each feature check each of the contingency feature
            for (int j = 0; j < testFeatures.length; j++) {
                if (i != j) {
                    String getScoreSQL = "With temp as( select sum(f.count) as s \n" +
                            " from piece as t, ff" + testFeatures[i] +" as f, ff" + testFeatures[j] + " as c " +
                            " where classifier( ";
                    for (int k = 0; k < testFeatures.length-1; k++) {
                        if (k == i){
                            getScoreSQL = getScoreSQL + "f." + testFeatures[k] + ", ";
                        } else if (k == j) {
                            getScoreSQL = getScoreSQL + "c." + testFeatures[k] + ", ";
                        } else {
                            getScoreSQL = getScoreSQL + "t." + testFeatures[k] + ", ";
                        }
                    }
                    if (i == 22) {
                        getScoreSQL = getScoreSQL + "f." + testFeatures[22] + " )= 0";
                    } else if (j == 22) {
                        getScoreSQL = getScoreSQL + "c." + testFeatures[22] + " )= 0";
                    } else {
                        getScoreSQL = getScoreSQL + "t." + testFeatures[22] + " )= 0";
                    }
                    getScoreSQL = getScoreSQL + " group by  c." + testFeatures[j] +
                            ") select max(s) from temp\n" +
                            ";";
                    PreparedStatement getScore = conn.prepareStatement(getScoreSQL);
                    ResultSet resultSet = getScore.executeQuery();
                    resultSet.next();
                    double currentScore = resultSet.getInt(1);
                    if (score < currentScore) {
                        score = currentScore;
                    }
                    resultSet.close();
                }
            }
            score = score / trainSize /2;
            scores[i] = score;
        }
        return scores;
    }

    /**
     *  delete the original test entity and insert the new test entity into the test table piece -- this aims to use the
     *  sql joins to speed up.
     *
     * @param conn the connection to the database
     * @param testFeatures the features of the test entity
     * @param testEntitySet the test piece
     * @throws SQLException this should happen if connect is correct with valid entity
     */
    public static void insertTestData(Connection conn, String[] testFeatures, ResultSet testEntitySet)
            throws SQLException {
        // to make sure that the table is empty before
        String deletePieceSQL = "delete from piece;";
        PreparedStatement deletePiece = conn.prepareStatement(deletePieceSQL);
        deletePiece.execute();
        // insert the test piece into the table to test
        String insertPieceSQL = "INSERT INTO piece VALUES (";
        for (int i = 1; i < testFeatures.length; i++) {
            insertPieceSQL  = insertPieceSQL + testEntitySet.getInt(i) + ", ";
        }
        insertPieceSQL = insertPieceSQL + testEntitySet.getInt(testFeatures.length) + ");";
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
    public static void getMostImportantFeature(double[] scores, int[] mostImportantFeature,
                                               int[] most4ImportantFeature){

        int count = 0; // the number of valid non-zero features
        double[] most1 = new double[]{-1, 0};
        double[] most2 = new double[]{-1, 0};
        double[] most3 = new double[]{-1, 0};
        double[] most4 = new double[]{-1, 0};
        for (int i = 0; i < scores.length; i++) {
            if (scores[i] == 0) {
                count++; // check whether still all zeros
            } else {
                if (scores[i] > most1[1]){
                    most4[0] = most3[0];
                    most4[1] = most3[1];
                    most3[0] = most2[0];
                    most3[1] = most2[1];
                    most2[0] = most1[0];
                    most2[1] = most1[1];
                    most1[0] = i;
                    most1[1] = scores[i];
                } else if (scores[i] > most2[1]){
                    most4[0] = most3[0];
                    most4[1] = most3[1];
                    most3[0] = most2[0];
                    most3[1] = most2[1];
                    most2[0] = i;
                    most2[1] = scores[i];
                } else if (scores[i] > most3[1]){
                    most4[0] = most3[0];
                    most4[1] = most3[1];
                    most3[0] = i;
                    most3[1] = scores[i];
                } else if (scores[i] > most4[1]){
                    most4[0] = i;
                    most4[1] = scores[i];
                }
            }
        }
        // check whether is enough valid features
        if (count != 23) {
            mostImportantFeature[(int) most1[0]]++;
            most4ImportantFeature[(int) most1[0]]++;
            if (most2[0] != -1) {
                most4ImportantFeature[(int) most2[0]]++;
                if (most3[0] != -1) {
                    most4ImportantFeature[(int) most3[0]]++;
                    if (most4[0] != -1) {
                        most4ImportantFeature[(int) most4[0]]++;
                    }
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
     * create the classifier in the database
     *
     * @param conn the connection to the database
     */
    public static void createClassifier(Connection conn) throws SQLException {
        String functionForFeatureSQL = "create function feternal_risk_subscore_from_index(\n" +
                "\tindex int\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights float[] := '{2.9895622, 2.1651128, 1.4081029, 0.7686735, 0, 0, 0, 1.6943381}';\n" +
                "  score_1 float := -1.4308699;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  score_1+weights[index];\n" +
                "  raw := 1.5671672 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "create function ftrade_open_time_subscore_from_index(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights1 float[] := '{8.20842027e-01, 0.525120503, 0.245257364, 0.005524848, 0, 0.418318111, 0.435851213}';\n" +
                "  weights2 float[] := '{0.031074792, 0.006016629, 0, 0, 0.027688067}';\n" +
                "  weights3 float[] := '{1.209930852, 0.694452470, 0.296029824, 0, 0, 0, 0.471490736}';\n" +
                "  weight float := -0.696619002;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw := weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);\n" +
                "  raw := 2.5236825 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "create function fnum_sat_trades_subscore_from_index(\n" +
                "  index int\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights float[] := '{2.412574, 1.245278, 0.6619963, 0.2731984, 5.444148e-09, 0, 0, 0.4338848}';\n" +
                "  score_1 float := -0.1954726;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  score_1+weights[index];\n" +
                "  raw := 2.1711503 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "create function ftrade_freq_subscore_from_index(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights1 float[] := '{2.710260e-04, 9.195886e-01, 9.758620e-01, 1.008107e+01, 9.360290, 0, 0, 3.970360e-01}';\n" +
                "  weights2 float[] := '{1.514937e-01, 3.139667e-01, 0, 2.422345e-01, 0, 0, 3.095043e-02}';\n" +
                "  weights3 float[] := '{2.888436e-01, 9.659472e-01, 5.142479e-01, 2.653203e-01, 8.198233e-07, 0, 0, 3.233593e-01}';\n" +
                "  weights4 float[] := '{8.405069e-06, 3.374686e-01, 4.934466e-01, 8.601860e-01, 9.451724, 0, 0, 1.351433e-01}';\n" +
                "  weight float := -6.480598e-01;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]] + weights4[index[4]]);\n" +
                "  raw :=  0.3323177 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "create  function fdelinquency_subscore_from_index(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights1 float[] := '{1.658975, 1.218405, 8.030501e-01, 5.685712e-01, 0, 0, 0, 6.645698e-01}';\n" +
                "  weights2 float[] := '{4.014945e-01, 2.912651e-01, 5.665418e-02, 0,6.935965e-01, 5.470874e-01, 4.786956e-01}';\n" +
                "  weights3 float[] := '{1.004642, 5.654694e-01, 0, 0, 0, 2.841047e-01}';\n" +
                "  weight float := -1.199469;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);\n" +
                "  raw := 2.5396631 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "create function fnstallment_subscore_from_index(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  raw float := 0;\n" +
                "  weights1 float[] := '{9.059412e-05, 1.292266e-01, 4.680034e-01, 8.117938e-01, 1.954441, 0, 0, 1.281830}';\n" +
                "  weights2 float[] := '{0, 1.432068e-01, 3.705526e-01, 0, 4.972869e-03, 1.513885e-01}';\n" +
                "  weights3 float[] := '{1.489759, 1.478176, 1.518328, 0, 9.585058e-01, 0, 1.506442, 5.561296e-01}';\n" +
                "  weight float := -1.750937;\n" +
                "begin\n" +
                "  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);\n" +
                "  raw := 0.9148520 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "create function finquiry_subscore_from_inedx(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights1 float[] := '{1.907737, 1.260966, 1.010585, 8.318137e-01, 0, 1.951357, 0, 1.719356}';\n" +
                "  weights2 float[] := '{2.413596e-05, 2.251582e-01, 5.400251e-01, 1.255076, 0, 0, 1.061504e-01}';\n" +
                "  weights3 float[] := '{0, 6.095516e-02, 0, 0, 1.125418e-02}';\n" +
                "  weight float := -1.598351;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  weight+(weights1[index[1]] + weights2[index[2]] + weights3[index[3]]);\n" +
                "  raw := 3.0015073 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "create function frevol_balance_subscore_from_index(\n" +
                "\tindex int[]\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights1 float[] := '{0.0001042232, 0.6764476961, 1.3938464180, 2.2581926077, 0, 1.7708134303, 1.0411847907}';\n" +
                "  weights2 float[] := '{ 0.0756555085, 0, 0.1175915408, 0.2823307493, 0.4242649887, 0, 0.8756715032, 0.0897134843}';\n" +
                "  weight float := -0.8924856930;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  weight+(weights1[index[1]] + weights2[index[2]]);\n" +
                "  raw := 1.9259728 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "\n" +
                "create function futilization_subscore_from_index(\n" +
                "\tindex int\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights float[] := '{0, 0.8562096, 1.2047649, 1.1635459, 1.4701220, 0, 1.2392294, 0.4800086}';\n" +
                "  score_1 float := -0.2415871;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  score_1+weights[index];\n" +
                "  raw := 0.9864329 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n" +
                "\n" +
                "\n" +
                "create function ftrade_w_balance_subscore_from_index(\n" +
                "\tindex int\n" +
                ")\n" +
                "returns float as $$\n" +
                "DECLARE\n" +
                "  weights float[] := '{0, 0.5966752, 0.9207121, 1.2749998, 1.8474869, 0, 2.2885183, 1.0606029}';\n" +
                "  score_1 float := -0.8221922;\n" +
                "  raw float := 0;\n" +
                "begin\n" +
                "  raw :=  score_1+weights[index];\n" +
                "  raw := 0.2949793 / (1+exp(-raw));\n" +
                "  return raw;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;";
        String createClassifierSQL = "-- this is the classifier with pre-bucket data\n" +
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
                "index_1 int := 0;\n" +
                "index_2 int[3];\n" +
                "index_3 int := 0;\n" +
                "index_4 int[4];\n" +
                "index_5 int[4];\n" +
                "index_6 int[3];\n" +
                "index_7 int[3];\n" +
                "index_8 int[2];\n" +
                "index_9 int := 0;\n" +
                "index_10 int := 0;\n" +
                "score float := -8.3843046;\n" +
                "begin\n" +
                "index_1:= f1;\n" +
                "index_2[1] = f2;\n" +
                "index_2[2] = f3;\n" +
                "index_2[3] = f4;\n" +
                "index_3:= f5;\n" +
                "index_4[1] = f6;\n" +
                "index_4[2] = f7;\n" +
                "index_4[3] = f12;\n" +
                "index_4[4] = f13;\n" +
                "index_5[1] = f8;\n" +
                "index_5[2] = f9;\n" +
                "index_5[3] = f10;\n" +
                "index_5[4] = f11;\n" +
                "index_6[1] = f14;\n" +
                "index_6[2] = f19;\n" +
                "index_6[3] = f21;\n" +
                "index_7[1] = f15;\n" +
                "index_7[2] = f16;\n" +
                "index_7[3] = f17;\n" +
                "index_8[1] = f18;\n" +
                "index_8[2] = f20;\n" +
                "index_9:= f22;\n" +
                "index_10:= f23;\n" +
                "score := score + fexternal_risk_subscore_from_index(index_1) + ftrade_open_time_subscore_from_index(index_2)\n" +
                "        + fnum_sat_trades_subscore_from_index(index_3) + ftrade_freq_subscore_from_index(index_4)\n" +
                "        + fdelinquency_subscore_from_index(index_5) + finstallment_subscore_from_index(index_6)\n" +
                "        + finquiry_subscore_from_inedx(index_7) + frevol_balance_subscore_from_index(index_8)\n" +
                "        + futilization_subscore_from_index(index_9) + ftrade_w_balance_subscore_from_index(index_10);\n" +
                "score:= 1 / (1+exp(-score));\n" +
                "IF (score < 0.5) THEN\n" +
                "\treturn 0;\n" +
                "ELSE\n" +
                "\treturn 1;\n" +
                "END IF;\n" +
                "end;\n" +
                "$$\n" +
                "language plpgsql;\n";
        PreparedStatement functionForFeature = conn.prepareStatement(functionForFeatureSQL);
        PreparedStatement createClassifier = conn.prepareStatement(createClassifierSQL);
        functionForFeature.execute();
        createClassifier.execute();
        conn.commit();
        System.out.println("1");
    }
}
