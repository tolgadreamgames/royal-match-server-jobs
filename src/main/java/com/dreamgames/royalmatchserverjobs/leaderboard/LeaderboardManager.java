package com.dreamgames.royalmatchserverjobs.leaderboard;

import com.dreamgames.royalmatchserverjobs.db.DBConnectionManager;
import com.dreamgames.royalmatchserverjobs.redis.RedisManager;
import com.dreamgames.royalmatchserverjobs.util.Tuple;
import com.dreamgames.royalmatchserverjobs.util.UtilHelper;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LeaderboardManager {
    private static final int LEADERBOARD_SIZE = 20;
    private static final List<String> activeCountries = new ArrayList<>(Arrays.asList("TR", "GB"));

    public static void process() throws Exception {
        var stringBuilder = new StringBuilder();
        activeCountries.forEach(ac -> stringBuilder.append(ac).append(", "));

        //players leaderboard
        System.err.println("Getting players leaderboard for active countries: " + stringBuilder.toString());
        var playersLeaderboard = getNonLeaguePlayerLeaderboardByCountry();
        var countrySizeForPlayers = playersLeaderboard.size();
        System.err.println("Got players leaderboard for active " + countrySizeForPlayers + " countries");

        playersLeaderboard.put("LOCAL", getNonLeaguePlayerLeaderboardForLocal());
        System.err.println("Got players leaderboard for local");

        var usPlayerLeaderboard = getNonLeaguePlayerLeaderboardForUsStates();
        var sizeUsStatesForPlayer = playersLeaderboard.size() - countrySizeForPlayers - 1;
        System.err.println("Got players leaderboard for " + sizeUsStatesForPlayer + " us states");

        usPlayerLeaderboard.forEach((key, value) -> playersLeaderboard.put(key, value));

        playersLeaderboard.put("WORLD", getNonLeaguePlayerLeaderboardForWorld());
        System.err.println("Got players leaderboard for world");

        RedisManager.fillPlayerLeaderboard(playersLeaderboard);
        System.err.println("Fill players leaderboard operation is completed");


        //teams leaderboard
        System.err.println("Getting teams leaderboard for active countries: " + stringBuilder.toString());
        var teamsLeaderboard = getNonLeagueTeamLeaderboardByCountry();
        var countrySizeForTeams = teamsLeaderboard.size();
        System.err.println("Got teams leaderboard for active " + countrySizeForTeams + " countries");

        teamsLeaderboard.put("LOCAL", getNonLeagueTeamLeaderboardForLocal());
        System.err.println("Got teams leaderboard for local");

        var usTeamLeaderboard = getNonLeagueTeamLeaderboardForUsStates();
        var sizeUsStatesForTeam = playersLeaderboard.size() - countrySizeForTeams - 1;
        System.err.println("Got players leaderboard for " + sizeUsStatesForTeam + " us states");

        usTeamLeaderboard.forEach((key, value) -> teamsLeaderboard.put(key, value));

        teamsLeaderboard.put("WORLD", getNonLeagueTeamLeaderboardForWorld());
        System.err.println("Got teams leaderboard for world");

        RedisManager.fillTeamLeaderboard(teamsLeaderboard);
        System.err.println("Fill teams leaderboard operation is completed");
    }

    private static String GET_NON_LEAGUE_TOP_USERS_BY_COUNTRY = "SELECT user_id, level, country, last_level_update_date FROM users WHERE country = ? ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static Map<String, List<Tuple<Long, Double>>> getNonLeaguePlayerLeaderboardByCountry() throws Exception {
        Map<String, List<Tuple<Long, Double>>> map = Maps.newHashMap();

        for (String activeCountry : activeCountries) {
            if (activeCountry.equalsIgnoreCase("US")) {
                continue;
            }
            final String country = activeCountry;
            map.put(country, new ArrayList<>());
            ResultSet rs = null;
            try (Connection con = DBConnectionManager.getReadonlyConnection();
                 PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_USERS_BY_COUNTRY)) {

                pst.setString(1, country);
                rs = pst.executeQuery();
                while (rs.next()) {
                    map.get(rs.getString("country")).add(
                            new Tuple<>(rs.getLong("user_id"),
                                    UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            } finally {
                rs.close();
            }
        }

        return map;
    }

    private static String GET_NON_LEAGUE_TOP_USERS_FOR_US_STATES = "SELECT user_id, level, country_state, last_level_update_date FROM users WHERE country = 'US' and country_state <> '' ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static Map<String, List<Tuple<Long, Double>>> getNonLeaguePlayerLeaderboardForUsStates() throws Exception {
        Map<String, List<Tuple<Long, Double>>> map = Maps.newHashMap();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_USERS_FOR_US_STATES);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                var state = "US-" + rs.getString("country_state");

                if (!map.containsKey(state)) {
                    map.put(state, new ArrayList<>());
                }

                map.get(state).add(
                        new Tuple<>(rs.getLong("user_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return map;
    }

    private static String GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_FIRST_PART = "SELECT user_id, level, country, last_level_update_date FROM users WHERE ";
    private static String GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_SECOND_PART = " ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static List<Tuple<Long, Double>> getNonLeaguePlayerLeaderboardForLocal() throws Exception {
        List<Tuple<Long, Double>> leaderboard = new ArrayList<>();

        var stringBuilder = new StringBuilder(GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_FIRST_PART);
        for (String activeCountry : activeCountries) {
            stringBuilder.append("country <> '").append(activeCountry).append("' and ");
        }
        stringBuilder.setLength(stringBuilder.length() - 4);
        stringBuilder.append(GET_NON_LEAGUE_TOP_USERS_FOR_LOCAL_SECOND_PART);

        var query = stringBuilder.toString();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(query);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                leaderboard.add(
                        new Tuple<>(rs.getLong("user_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return leaderboard;
    }

    private static String GET_NON_LEAGUE_TOP_USERS_FOR_WORLD = "SELECT user_id, level, last_level_update_date FROM users  ORDER BY level DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static List<Tuple<Long, Double>> getNonLeaguePlayerLeaderboardForWorld() throws Exception {
        List<Tuple<Long, Double>> leaderboard = new ArrayList<>();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_USERS_FOR_WORLD);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                leaderboard.add(
                        new Tuple<>(rs.getLong("user_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("level"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return leaderboard;
    }

    private static String GET_NON_LEAGUE_TOP_TEAMS_BY_COUNTRY = "SELECT team_id, score, country, last_level_update_date FROM teams WHERE country = ? ORDER BY score DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static Map<String, List<Tuple<Long, Double>>> getNonLeagueTeamLeaderboardByCountry() throws Exception {
        Map<String, List<Tuple<Long, Double>>> map = Maps.newHashMap();

        for (String activeCountry : activeCountries) {
            if (activeCountry.equalsIgnoreCase("US")) {
                continue;
            }
            final String country = activeCountry;
            map.put(country, new ArrayList<>());
            ResultSet rs = null;
            try (Connection con = DBConnectionManager.getReadonlyConnection();
                 PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_TEAMS_BY_COUNTRY)) {

                pst.setString(1, country);
                rs = pst.executeQuery();
                while (rs.next()) {
                    map.get(rs.getString("country")).add(
                            new Tuple<>(rs.getLong("team_id"),
                                    UtilHelper.wrapScoreWithDate(rs.getInt("score"), rs.getTimestamp("last_level_update_date").getTime())));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            } finally {
                rs.close();
            }
        }

        return map;
    }

    private static String GET_NON_LEAGUE_TOP_TEAMS_FOR_US_STATES = "SELECT team_id, score, country_state, last_level_update_date FROM teams WHERE country = 'US' and country_state <> '' ORDER BY score DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static Map<String, List<Tuple<Long, Double>>> getNonLeagueTeamLeaderboardForUsStates() throws Exception {
        Map<String, List<Tuple<Long, Double>>> map = Maps.newHashMap();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_TEAMS_FOR_US_STATES);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                var state = "US-" + rs.getString("country_state");

                if (!map.containsKey(state)) {
                    map.put(state, new ArrayList<>());
                }

                map.get(state).add(
                        new Tuple<>(rs.getLong("team_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("score"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return map;
    }

    private static String GET_NON_LEAGUE_TOP_TEAMS_FOR_LOCAL_FIRST_PART = "SELECT team_id, score, country, last_level_update_date FROM teams WHERE ";
    private static String GET_NON_LEAGUE_TOP_TEAMS_FOR_LOCAL_SECOND_PART = " ORDER BY score DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static List<Tuple<Long, Double>> getNonLeagueTeamLeaderboardForLocal() throws Exception {
        List<Tuple<Long, Double>> leaderboard = new ArrayList<>();

        var stringBuilder = new StringBuilder(GET_NON_LEAGUE_TOP_TEAMS_FOR_LOCAL_FIRST_PART);
        for (String activeCountry : activeCountries) {
            stringBuilder.append("country <> '").append(activeCountry).append("' and ");
        }
        stringBuilder.setLength(stringBuilder.length() - 4);
        stringBuilder.append(GET_NON_LEAGUE_TOP_TEAMS_FOR_LOCAL_SECOND_PART);

        var query = stringBuilder.toString();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(query);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                leaderboard.add(
                        new Tuple<>(rs.getLong("team_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("score"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return leaderboard;
    }

    private static String GET_NON_LEAGUE_TOP_TEAMS_FOR_WORLD = "SELECT team_id, score, last_level_update_date FROM teams  ORDER BY score DESC, last_level_update_date LIMIT " + LEADERBOARD_SIZE;

    private static List<Tuple<Long, Double>> getNonLeagueTeamLeaderboardForWorld() throws Exception {
        List<Tuple<Long, Double>> leaderboard = new ArrayList<>();

        try (Connection con = DBConnectionManager.getReadonlyConnection();
             PreparedStatement pst = con.prepareStatement(GET_NON_LEAGUE_TOP_TEAMS_FOR_WORLD);
             ResultSet rs = pst.executeQuery()) {

            while (rs.next()) {
                leaderboard.add(
                        new Tuple<>(rs.getLong("team_id"),
                                UtilHelper.wrapScoreWithDate(rs.getInt("score"), rs.getTimestamp("last_level_update_date").getTime())));
            }
        } catch (Exception e) {
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

/**
 * Calculate non league team score
 * SELECT team_members.team_id, sum(users.level) as score, max(last_level_update_date) as last_level_update_date
 * FROM team_members
 * LEFT JOIN users ON team_members.user_id = users.user_id
 * GROUP BY team_members.team_id
 * ORDER BY score DESC
 * LIMIT 10
 */

