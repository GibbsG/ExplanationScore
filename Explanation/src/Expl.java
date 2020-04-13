import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Expl {
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn =  connect();
        conn.setAutoCommit(false);
        String filePath = "/Users/gzx/Desktop/research/fico/heloc_dataset_v2.csv";
//      String[] features =   createTable(filePath, "data");  // get features of the data
//      int length = insert(filePath, data) // get the total number of tuples

        String[] features  = new String[]{"RiskPerformance", "ExternalRiskEstimate", "MSinceOldestTradeOpen", "MSinceMostRecentTradeOpen",
                "AverageMInFile", "NumSatisfactoryTrades", "NumTrades60Ever2DerogPubRec", "NumTrades90Ever2DerogPubRec",
                "PercentTradesNeverDelq", "MSinceMostRecentDelq", "MaxDelq2PublicRecLast12M", "MaxDelqEver",
                "NumTotalTrades", "NumTradesOpeninLast12M", "PercentInstallTrades", "MSinceMostRecentInqexcl7days",
                "NumInqLast6M", "NumInqLast6Mexcl7days", "NetFractionRevolvingBurden", "NetFractionInstallBurden",
                "NumRevolvingTradesWBalance", "NumInstallTradesWBalance", "NumBank2NatlTradesWHighUtilization",
                "PercentTradesWBalance"
        };
        String[] testFeatures  = new String[]{ "ExternalRiskEstimate", "MSinceOldestTradeOpen", "MSinceMostRecentTradeOpen",
                "AverageMInFile", "NumSatisfactoryTrades", "NumTrades60Ever2DerogPubRec", "NumTrades90Ever2DerogPubRec",
                "PercentTradesNeverDelq", "MSinceMostRecentDelq", "MaxDelq2PublicRecLast12M", "MaxDelqEver",
                "NumTotalTrades", "NumTradesOpeninLast12M", "PercentInstallTrades", "MSinceMostRecentInqexcl7days",
                "NumInqLast6M", "NumInqLast6Mexcl7days", "NetFractionRevolvingBurden", "NetFractionInstallBurden",
                "NumRevolvingTradesWBalance", "NumInstallTradesWBalance", "NumBank2NatlTradesWHighUtilization",
                "PercentTradesWBalance"
        };
        // sample the test data and training data
        int length = 10459; // get the size of the test data
        //int testDataSize = 1975;
        int seed = 233; //create a seed for better use
        Random rand = new Random(234);
        //sampleData(length, rand, filePath);
        int trainSize = 7883;
        // if value = 0, start size with 1, 2, 3
        // get the largest several feature.


        // create the view that will used later
        //createView(conn, features);

        // Let assume L, the identifier to be the sum of all entities larger than 500 to be good;
        // first get all the test entity
//        ResultSet testEntitySet =  getTestEntity(conn);
//
//         //then for each of the entity with bad outcome, we find the result
//        while (testEntitySet.next()) {
//            if (testEntitySet.getInt("RiskPerformance") == 1) {
//                double[] scores = new double[features.length];
//                // do the test for each of the feature
//                int[] featureValue = new int[features.length];
//                for (int i = 0; i < featureValue.length; i++) {
//                    featureValue[i] = testEntitySet.getInt(i+1);
//                }
//                for (int i = 1; i < featureValue.length; i++) {
//
//                    //
//                    double score = 0;
//                    String getAllFeaturesSQL = "select * from F" + features[i] +";";
//                    PreparedStatement getAllFeatures = conn.prepareStatement(getAllFeaturesSQL);
//                    ResultSet resultSet = getAllFeatures.executeQuery();
//                    int[] values = featureValue.clone();
//                    // if the result == 0, score + count
//                    while (resultSet.next()) {
//                        values[i] = resultSet.getInt(1);
//                        if (classifier(values) == 0) {
//                            score += resultSet.getInt(2);
//                        }
//                    }
//                    resultSet.close();
//                    // check whether the score is 0 and need recompute with contingency set size 1
//                    // the contingency set can be any feature not the current feature -- feature[i]
//                    if (score==0) {
//                        // todo: set for the contigency set with better value;
//                    }
//                    score = score / trainSize;
//                    scores[i] = score;
//
//                }
//                System.out.println(Arrays.toString(scores));
//            }
//        }
//
//
//        testEntitySet.close();











        int mostImportantFeature[] = new int[23];
        int most4ImportantFeature[] = new int[23];

        ResultSet testEntitySet =  getTestEntity(conn);
        int zeroNo = 0;
        int zero = 0;
        int zero2 = 0;
         //then for each of the entity with bad outcome, we find the result
        while (testEntitySet.next()) {

                double[] scores = new double[testFeatures.length];
                int count = 0;
                int check = 0;
                // to make sure that the table is empty before
                String deletePieceSQL = "delete from piece;";
                PreparedStatement deletePiece = conn.prepareStatement(deletePieceSQL);
                deletePiece.execute();
                // insert the test piece into the table to test
                String insertPieceSQL = "INSERT INTO piece VALUES (";
                for (int i = 2; i < features.length; i++) {
                    insertPieceSQL  = insertPieceSQL + testEntitySet.getInt(i) + ", ";
                }
                insertPieceSQL = insertPieceSQL + testEntitySet.getInt(features.length) + ");";
                PreparedStatement insertPiece = conn.prepareStatement(insertPieceSQL);
                insertPiece.execute();
                // check whether the input lable right
                String checkLabelSQL = "select function4( t.ExternalRiskEstimate, t.MSinceOldestTradeOpen, t.MSinceMostRecentTradeOpen, t.AverageMInFile, t.NumSatisfactoryTrades, t.NumTrades60Ever2DerogPubRec, t.NumTrades90Ever2DerogPubRec, t.PercentTradesNeverDelq, t.MSinceMostRecentDelq, t.MaxDelq2PublicRecLast12M, t.MaxDelqEver, t.NumTotalTrades, t.NumTradesOpeninLast12M, t.PercentInstallTrades, t.MSinceMostRecentInqexcl7days, t.NumInqLast6M, t.NumInqLast6Mexcl7days, t.NetFractionRevolvingBurden, t.NetFractionInstallBurden, t.NumRevolvingTradesWBalance, t.NumInstallTradesWBalance, t.NumBank2NatlTradesWHighUtilization, t.PercentTradesWBalance ) \n" +
                        "from piece as t;";
                PreparedStatement checkLabel = conn.prepareStatement(checkLabelSQL);
                ResultSet resultSetForLabel = checkLabel.executeQuery();
                resultSetForLabel.next();
                int label = resultSetForLabel.getInt(1);
                resultSetForLabel.close();
                conn.commit();
                //only check for label == 1
                if (label == 1) {
                    for (int i = 0; i < testFeatures.length; i++) {
                        String getScoreSQL = "select sum(f.count) \n" +
                                "from piece as t, " + testFeatures[i] +"_b as f " +
                                "where function4( ";
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
                        if (score == 1) {
                            check++;
                        }
                        if (score == 0) {
                            count++;
                        }
                    }
                    if (count == 23) {
                        int ze = 0;
                        // start with contingency set size 1
                        for (int i = 0; i < testFeatures.length; i++) {
                            double score = 0;
                            // for each feature check each of the contingency feature
                            for (int j = 0; j < testFeatures.length; j++) {
                                if (i != j) {
                                    String getScoreSQL = "With temp as( select sum(f.count) as s \n" +
                                            " from piece as t, " + testFeatures[i] +"_b as f, " + testFeatures[j] + "_b as c " +
                                            " where function4( ";
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
                            if (score == 0) {
                                ze++;
                            }
                        }
                        // the score is still 0 even with the contingency size of 1
                        if (ze==23) {
                            zero2++;
                        }
                        zero++;
                    }
                    else {
                        zeroNo++;
                    }
                    // generate the result
                    double most1[] = new double[]{-1, 0};
                    double most2[] = new double[]{-1, 0};
                    double most3[] = new double[]{-1, 0};
                    double most4[] = new double[]{-1, 0};
                    count = 0;
                    for (int i = 0; i < testFeatures.length; i++) {
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







        conn.close();
    }

    public static int classifier(int[] values){
        int sum = 0;
        for (int value: values) {
            sum += value;
        }
        if (sum < 520) {
            return 1;
        }
        return 0;
    }

    public static void createView(Connection conn, String[] features) throws SQLException {
        // create view of each feature;
        for (int i = 1; i < features.length; i++) {
            String createViewSql = "CREATE VIEW " + "F" + features[i] + " as select " + features[i] + ", count(*) from " +
                    "trainData group by " + features[i] + ";";
            PreparedStatement createView = conn.prepareStatement(createViewSql);
            createView.executeUpdate();
        }
        conn.commit();
    }

    public static void sampleData(int length, Random rand, String filePath) throws SQLException, IOException {
        Set<Integer> testData = new HashSet<>();
        while (testData.size() < length/5) {
            testData.add(rand.nextInt(length));
        }
        String testTableName = "testData";
        String trainTableName = "trainData";
        createTable(filePath, testTableName);
        createTable(filePath, trainTableName);
        insertForTest(filePath, testTableName, trainTableName, testData);
    }

    public static ResultSet getTestEntity(Connection conn) throws SQLException {
        String getEntitySql = "Select * from testData;";
        PreparedStatement getEntity = conn.prepareStatement(getEntitySql);
        ResultSet result = getEntity.executeQuery();
        return result;
    }

    //create the table
    public static String[] createTable(String path, String tableName) throws IOException, SQLException {
        Connection conn =  connect();
        conn.setAutoCommit(false);

        BufferedReader reader = null;
        String[] names = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            names = reader.readLine().split(",");
            String createSQL = "CREATE TABLE " + tableName + " (" + names[0] + " int";
            for (int i = 1; i < names.length; i++) {
                createSQL = createSQL + ", "  + names[i] + " int";
            }
            System.out.println(createSQL);
            System.out.println(names.length);
            createSQL = createSQL + ");";
            PreparedStatement createTable = conn.prepareStatement(createSQL);
            createTable.execute();
            conn.commit();
            conn.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        conn.close();
        return names;
    }

    // insert the file, the data must be pre-filtered that only contain int as values
    public static void insert(String path, String tableName) throws SQLException {
        String insertSQL = null;
        Connection conn =  connect();
        conn.setAutoCommit(false);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            String line = null;

            int length =  reader.readLine().split(",").length;
            insertSQL = "INSERT INTO "+ tableName + "  VALUES ( ?";
            for (int i = 1; i < length; i++) {
                insertSQL = insertSQL + ", ?";
            }
            insertSQL = insertSQL + ");";
            PreparedStatement insertValue = conn.prepareStatement(insertSQL);
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                for (int i = 0; i < length; i++) {
                    insertValue.setInt(i+1, Integer.parseInt(data[i]));
                }
                insertValue.execute();
            }
            System.out.println(insertSQL);
            conn.commit();
            conn.close();
        } catch (SQLException | IOException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
                System.out.println(insertSQL);
                e.printStackTrace();
            }
        }
    }

    // insert the file, the data must be pre-filtered that only contain int as values
    public static void insertForTest(String path, String testTableName, String trainTableName, Set<Integer> testSet) throws SQLException {
        String insertTestSQL = null;
        Connection conn =  connect();
        conn.setAutoCommit(false);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException io) {
            System.out.println("FileNoFound");
        }
        try {
            String line = null;
            int length =  reader.readLine().split(",").length;
            insertTestSQL = "INSERT INTO "+ testTableName + "  VALUES ( ?";
            String insertTrainSQL = "INSERT INTO "+ trainTableName + "  VALUES ( ?";

            for (int i = 1; i < length; i++) {
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
                if (Integer.parseInt(data[1]) != -9) { // remove the missing data
                    if (testSet.contains(index)) {
                        for (int i = 0; i < length; i++) {
                            insertTestValue.setInt(i+1, Integer.parseInt(data[i]));
                        }
                        insertTestValue.execute();
                    } else {
                        for (int i = 0; i < length; i++) {
                            insertTrainValue.setInt(i+1, Integer.parseInt(data[i]));
                        }
                        insertTrainValue.execute();
                    }
                }
                index++;
            }
            conn.commit();
            conn.close();
        } catch (SQLException | IOException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // connect to the database
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
