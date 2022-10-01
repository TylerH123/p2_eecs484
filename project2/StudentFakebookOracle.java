package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.swing.text.html.HTML.Tag;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    // (B) Find the birth month in which the most users were born
    // (C) Find the birth month in which the fewest users (at least one) were born
    // (D) Find the IDs, first names, and last names of users born in the month
    // identified in (B)
    // (E) Find the IDs, first names, and last name of users born in the month
    // identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find
    // the appropriate
    // mechanisms for opening up a statement, executing a query, walking through
    // results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that
                                                                    // birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month,
                                                                          // descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); // it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); // it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    // (B) The first name(s) with the fewest letters
    // (C) The first name held by the most users
    // (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT MAX(LENGTH(First_Name)), MIN(LENGTH(First_Name)) " +
                            "FROM " + UsersTable);

            int max = 0;
            int min = 0;
            while (rst.next()) {
                max = rst.getInt(1);
                min = rst.getInt(2);
            }

            FirstNameInfo info = new FirstNameInfo();

            rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                            "FROM " + UsersTable + " " +
                            "WHERE LENGTH(First_Name) = " + max + " " +
                            "ORDER BY First_Name");
            while (rst.next()) {
                info.addLongName(rst.getString(1));
            }

            rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                            "FROM " + UsersTable + " " +
                            "WHERE LENGTH(First_Name) = " + min + " " +
                            "ORDER BY First_Name");
            while (rst.next()) {
                info.addShortName(rst.getString(1));
            }

            rst = stmt.executeQuery(
                    "SELECT First_Name, COUNT(*) " +
                            "FROM " + UsersTable + " " +
                            "GROUP BY First_Name " +
                            "ORDER BY COUNT(*) DESC");

            int commonNameCount = -1;
            if (rst.next()) {
                commonNameCount = rst.getInt(2);
                info.addCommonName(rst.getString(1));
                info.setCommonNameCount(commonNameCount);
            }
            while (rst.next() && rst.getInt(2) == commonNameCount) {
                info.addCommonName(rst.getString(1));
            }

            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any
    // friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only
    // contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " +
                            "FROM " + UsersTable + " " +
                            "WHERE User_ID NOT IN " +
                            "(SELECT DISTINCT U.User_ID " +
                            "FROM " + UsersTable + " U, " + FriendsTable + " F " +
                            "WHERE U.User_ID = F.User1_ID OR U.User_ID = F.User2_ID)" +
                            "ORDER BY USER_ID");

            while (rst.next()) {
                UserInfo u = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer
    // live
    // in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                            "FROM " + UsersTable + " U, " + CurrentCitiesTable + " C, " +
                            HometownCitiesTable + " H " +
                            "WHERE U.User_ID = C.User_ID AND U.User_ID = H.User_ID AND " +
                            "C.Current_City_ID IS NOT NULL AND H.Hometown_City_ID IS NOT NULL AND " +
                            "C.Current_City_ID != H.Hometown_City_ID " +
                            "ORDER BY U.User_ID");

            while (rst.next()) {
                UserInfo u = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of
    // the top
    // <num> photos with the most tagged users
    // (B) For each photo identified in (A), find the IDs, first names, and last
    // names
    // of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                    "SELECT inner.Tag_Photo_ID, P.Album_ID, P.Photo_Link, A.Album_Name " +
                            "FROM " + PhotosTable + " P, " + AlbumsTable + " A, ( " +
                            "SELECT innermost.Tag_Photo_ID, innermost.CT FROM ( " +
                            "SELECT Tag_Photo_ID, COUNT(*) AS CT " +
                            "FROM " + TagsTable + " " +
                            "GROUP BY Tag_Photo_ID " +
                            "ORDER BY CT DESC, Tag_Photo_ID ASC " +
                            ") innermost " +
                            "WHERE ROWNUM <= " + num +
                            " ) inner " +
                            "WHERE inner.Tag_Photo_ID = P.Photo_ID AND P.Album_ID = A.Album_ID " +
                            "ORDER BY inner.CT DESC, inner.Tag_Photo_ID ASC");

            try (Statement stmtInner = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                while (rst.next()) {
                    PhotoInfo p = new PhotoInfo(rst.getInt(1), rst.getInt(2), rst.getString(3),
                            rst.getString(4));
                    TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                    ResultSet inner = stmtInner.executeQuery(
                            "SELECT U.User_ID, U.First_Name, U.Last_Name " +
                                    "FROM " + UsersTable + " u, " + TagsTable + " T " +
                                    "WHERE U.User_ID = T.Tag_Subject_ID AND T.Tag_Photo_ID = " + rst.getInt(1));
                    while (inner.next()) {
                        UserInfo u = new UserInfo(inner.getInt(1), inner.getString(2),
                                inner.getString(3));
                        tp.addTaggedUser(u);
                    }
                    results.add(tp);
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
             * UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
             * UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
             * UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
             * TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
             * tp.addTaggedUser(u1);
             * tp.addTaggedUser(u2);
             * tp.addTaggedUser(u3);
             * results.add(tp);
             */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of
    // the two
    // users in the top <num> pairs of users that meet each of the following
    // criteria:
    // (i) same gender
    // (ii) tagged in at least one common photo
    // (iii) difference in birth years is no more than <yearDiff>
    // (iv) not friends
    // (B) For each pair identified in (A), find the IDs, links, and IDs and names
    // of
    // the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            // SELECT U1.USER_ID, U1.First_Name, U1.Last_Name, U1.Year_of_birth, U2.USER_ID,
            // U2.First_Name, U2.Last_Name, U2.Year_of_birth,
            // FROM project2.Public_Users U1, project2.Public_Users U2, (
            // SELECT T1.Tag_Subject_ID AS U1_ID, T2.Tag_Subject_ID AS U2_ID, COUNT(*)
            // FROM project2.Public_Tags T1, project2.Public_Tags T2,
            // (
            // SELECT DISTINCT U1.User_ID AS U1_ID, U2.User_ID AS U2_ID
            // FROM project2.Public_Users U1, project2.Public_Users U2, project2.Public_Tags
            // T1, project2.Public_Tags T2, project2.Public_Friends F
            // WHERE U1.gender = U2.gender AND T1.Tag_Photo_ID = T2.Tag_Photo_ID
            // AND T1.Tag_Subject_ID = U1.User_ID AND T2.Tag_Subject_ID = U2.User_ID
            // AND U1.User_ID < U2.User_ID AND U1.User_ID != F.User1_ID AND U2.User_ID !=
            // F.User2_ID
            // AND ABS(U1.Year_of_birth - U2.Year_of_birth) <= 2
            // ) innermost
            // WHERE T1.Tag_Subject_ID = innermost.U1_ID AND T2.Tag_Subject_ID =
            // innermost.U2_ID
            // AND T1.Tag_Photo_ID = T2.Tag_Photo_ID
            // GROUP BY T1.Tag_Subject_ID, T2.Tag_Subject_ID
            // ORDER BY COUNT(*) DESC, T1.Tag_Subject_ID, T2.Tag_Subject_ID
            // ) inner
            // WHERE U1.User_ID = inner.U1_ID AND U2.User_ID = inner.U2_ID
            // AND ROWNUM <= 2;

            ResultSet rst = stmt.executeQuery(
                    "SELECT U1.USER_ID, U1.First_Name, U1.Last_Name, U1.Year_of_birth, U2.USER_ID, U2.First_Name, U2.Last_Name, U2.Year_of_birth "
                            +
                            "FROM " + UsersTable + " U1, " + UsersTable + " U2, ( " +
                            "SELECT T1.Tag_Subject_ID AS U1_ID, T2.Tag_Subject_ID AS U2_ID, COUNT(*) " +
                            "FROM " + TagsTable + " T1, " + TagsTable + " T2, ( " +
                            "SELECT DISTINCT U1.User_ID AS U1_ID, U2.User_ID AS U2_ID " +
                            "FROM " + UsersTable + " U1, " + UsersTable + " U2, " + TagsTable + " T1, "
                            + TagsTable + " T2, " + FriendsTable + " F " +
                            "WHERE U1.gender = U2.gender AND T1.Tag_Photo_ID = T2.Tag_Photo_ID " +
                            "AND T1.Tag_Subject_ID = U1.User_ID AND T2.Tag_Subject_ID = U2.User_ID " +
                            "AND U1.User_ID < U2.User_ID AND U1.User_ID != F.User1_ID AND U2.User_ID != F.User2_ID " +
                            "AND ABS(U1.Year_of_birth - U2.Year_of_birth) <= " + yearDiff + " " +
                            ") innermost " +
                            "WHERE T1.Tag_Subject_ID = innermost.U1_ID AND T2.Tag_Subject_ID = innermost.U2_ID " +
                            "AND T1.Tag_Photo_ID = T2.Tag_Photo_ID " +
                            "GROUP BY U1_ID, U2_ID " +
                            "ORDER BY COUNT(*) DESC, T1.Tag_Subject_ID, T2.Tag_Subject_ID " +
                            ") inner " +
                            "WHERE U1.User_ID = inner.U1_ID AND U2.User_ID = inner.U2_ID " +
                            "AND ROWNUM <= " + num);

            try (Statement stmtInner = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                while (rst.next()) {
                    int u1_id = rst.getInt(1);
                    int u2_id = rst.getInt(5);

                    UserInfo u1 = new UserInfo(u1_id, rst.getString(2), rst.getString(3));
                    UserInfo u2 = new UserInfo(u2_id, rst.getString(6), rst.getString(7));
                    MatchPair mp = new MatchPair(u1, rst.getInt(4), u2, rst.getInt(8));

                    ResultSet inner = stmtInner.executeQuery(
                            "SELECT P.Photo_ID, P.Album_ID, P.Photo_Link, A.Album_Name " +
                                    "FROM " + PhotosTable + " P, " + TagsTable + " T1, " +
                                    TagsTable + " T2, " + AlbumsTable + " A " +
                                    "WHERE T1.Tag_Subject_ID = " + u1_id + " AND T2.Tag_Subject_ID = " + u2_id +
                                    "AND T1.Tag_Photo_ID = T2.Tag_Photo_ID " +
                                    "AND P.Photo_ID = T1.Tag_Photo_ID AND P.Album_ID = A.Album_ID " +
                                    "ORDER BY P.Photo_ID");
                    while (inner.next()) {
                        PhotoInfo p = new PhotoInfo(inner.getInt(1), inner.getInt(2), inner.getString(3),
                                inner.getString(4));
                        mp.addSharedPhoto(p);
                    }
                    results.add(mp);
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
             * UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
             * MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
             * PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
             * mp.addSharedPhoto(p);
             * results.add(mp);
             */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users
    // in
    // the top <num> pairs of users who are not friends but have a lot of
    // common friends
    // (B) For each pair identified in (A), find the IDs, first names, and last
    // names
    // of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(16, "The", "Hacker");
             * UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
             * UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
             * UsersPair up = new UsersPair(u1, u2);
             * up.addSharedFriend(u3);
             * results.add(up);
             */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are
    // held
    // (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            // SELECT C.State_Name, COUNT(*)
            // FROM project2.Public_Cities C, project2.Public_User_Events E
            // WHERE E.Event_City_ID = C.City_ID
            // GROUP BY C.State_Name
            // ORDER BY COUNT(*) DESC, C.State_Name;

            ResultSet rst = stmt.executeQuery(
                    "SELECT C.State_Name, COUNT(*) " +
                            "FROM " + CitiesTable + " C, " + EventsTable + " E " +
                            "WHERE E.Event_City_ID = C.City_ID " +
                            "GROUP BY C.State_Name " +
                            "ORDER BY COUNT(*) DESC, C.State_Name");

            int highest = 0;
            EventStateInfo info = new EventStateInfo(-1);
            if (rst.next()) {
                info = new EventStateInfo(highest);
                highest = rst.getInt(2);
                info.addState(rst.getString(1));
            }
            while (rst.next()) {
                if (rst.getInt(2) == highest) {
                    info.addState(rst.getString(1));
                } else {
                    break;
                }
            }

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * EventStateInfo info = new EventStateInfo(50);
             * info.addState("Kentucky");
             * info.addState("Hawaii");
             * info.addState("New Hampshire");
             * return info;
             */
            return info; // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the
    // user
    // with User ID <userID>
    // (B) Find the ID, first name, and last name of the youngest friend of the user
    // with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            // SELECT U1, U2 FROM (
            // SELECT DISTINCT USER_ID as U1
            // FROM project2.Public_Users
            // WHERE User_ID < 215
            // ) CROSS JOIN (
            // SELECT DISTINCT USER_ID as U2
            // FROM project2.Public_Users
            // WHERE User_ID = 215
            // ) UNION
            // SELECT U1, U2 FROM (
            // SELECT DISTINCT USER_ID as U1
            // FROM project2.Public_Users
            // WHERE User_ID = 215
            // ) CROSS JOIN (
            // SELECT DISTINCT USER_ID as U2
            // FROM project2.Public_Users
            // WHERE User_ID > 215
            // );

            // SELECT Author_ID, First_Name, Last_Name
            // FROM Authors
            // WHERE Author_ID NOT IN (
            // SELECT DISTINCT Author_ID FROM (
            // SELECT User_ID
            // FROM (
            // SELECT U1, U2 FROM (
            // SELECT DISTINCT USER_ID as U1
            // FROM project2.Public_Users
            // WHERE User_ID < 215
            // ) CROSS JOIN (
            // SELECT DISTINCT USER_ID as U2
            // FROM project2.Public_Users
            // WHERE User_ID = 215
            // ) UNION
            // SELECT U1, U2 FROM (
            // SELECT DISTINCT USER_ID as U1
            // FROM project2.Public_Users
            // WHERE User_ID = 215
            // ) CROSS JOIN (
            // SELECT DISTINCT USER_ID as U2
            // FROM project2.Public_Users
            // WHERE User_ID > 215
            // )
            // MINUS
            // ( SELECT DISTINCT A.Author_ID, S.Subject_ID
            // FROM Authors A, Books B, Subjects S
            // WHERE A.Author_ID = B.Author_ID AND B.Subject_ID = S.Subject_ID
            // )
            // )
            // )

            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
             * UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
             * return new AgeInfo(old, young);
             */
            return new AgeInfo(new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"), new UserInfo(-1, "UNWRITTEN", "UNWRITTEN")); // placeholder
                                                                                                                        // for
                                                                                                                        // compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    // (i) same last name
    // (ii) same hometown
    // (iii) are friends
    // (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
             * UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
             * SiblingInfo si = new SiblingInfo(u1, u2);
             * results.add(si);
             */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
