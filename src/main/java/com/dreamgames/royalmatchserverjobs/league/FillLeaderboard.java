package com.dreamgames.royalmatchserverjobs.league;

import com.dreamgames.royalmatchserverjobs.db.DBConnectionManager;
import com.dreamgames.royalmatchserverjobs.util.Tuple;
import com.dreamgames.royalmatchserverjobs.util.UtilHelper;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FillLeaderboard {
    private static final int LEADERBOARD_SIZE = 10;
    private static final List<String> activeCountries = new ArrayList<>(Arrays.asList("TR", "GB"));



    //TODO US state içinde ayrı çek
    public static void process() throws Exception {
        var leaderboard = getNonLeagueLeaderboardByCountry();
        leaderboard.put("LOCAL", getNonLeagueLeaderboardForLocal());

        System.err.println(leaderboard);

    }

    private static String GET_NON_LEAGUE_TOP_USERS_BY_COUNTRY = "SELECT user_id, level, country, last_level_update_date FROM users WHERE country = ? ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;
    private static Map<String, List<Tuple<Long, Double>>> getNonLeagueLeaderboardByCountry() throws Exception {
        Map<String, List<Tuple<Long, Double>>> map = Maps.newHashMap();

        for (String activeCountry : activeCountries) {
            final String country = activeCountry;
            map.put(country, new ArrayList<>());
            ResultSet rs = null;
            try(Connection con = DBConnectionManager.getReadonlyConnection();
                PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_USERS_BY_COUNTRY)) {

                pst.setString(1, country);
                rs = pst.executeQuery();
                while(rs.next()) {
                    map.get(rs.getString("country")).add(
                            new Tuple<Long, Double>(rs.getLong("user_id"),
                                    UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
                }
            }catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }finally {
                rs.close();
            }
        }

        return map;
    }

    private static String GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_FIRST_PART = "SELECT user_id, level, country, last_level_update_date FROM users WHERE ";
    private static String GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_SECOND_PART =" ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;
    private static List<Tuple<Long, Double>> getNonLeagueLeaderboardForLocal() throws Exception {
        List<Tuple<Long, Double>> leaderboard = new ArrayList<>();

        var stringBuilder = new StringBuilder(GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_FIRST_PART);
        for (String activeCountry : activeCountries) {
            stringBuilder.append("country <> '").append(activeCountry).append("' and ");
        }
        stringBuilder.setLength(stringBuilder.length() - 4);
        stringBuilder.append(GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_SECOND_PART);

        var query = stringBuilder.toString();

        try(Connection con = DBConnectionManager.getReadonlyConnection();
            PreparedStatement pst = con.prepareStatement(query);
            ResultSet rs = pst.executeQuery()) {

            while(rs.next()) {
                leaderboard.add(
                        new Tuple<Long, Double>(rs.getLong("user_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        }catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return leaderboard;
    }
}

/**
 * Worst case scenario query. Need to be optimized
 *
 * SELECT users.user_id, users.country, users.level, coalesce(league_members.level, 0) as league_level, coalesce(users.level+league_members.level, users.level) as score, users.last_level_update_date,  league_members.last_level_update_date as last_league_level_update_date, league_id
 * FROM users
 * LEFT JOIN league_members
 * ON users.user_id = league_members.user_id and league_members.league_id = 3 and users.country = 'TR'
 * ORDER BY league_members.level DESC, last_league_level_update_date
 * LIMIT 1000
 */

