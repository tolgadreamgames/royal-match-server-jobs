package com.dreamgames.royalmatchserverjobs.league;

import com.dreamgames.royalmatchserverjobs.db.DBConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FillLeaderboard {
    private static final int LEADERBOARD_SIZE = 1_000;
    private static final List<String> activeCountries = new ArrayList<>(Arrays.asList("TR", "GB"));

    private static String GET_TOP_USERS_BY_COUNTRY_FIRST_PART = "SELECT `user_id`, country, level, last_level_update_date FROM " +
            "(SELECT user_id, country, level, last_level_update_date, " +
            "@country_rank := IF(@current_country = country, @country_rank + 1, 1) AS country_rank, @current_country := country " +
            "FROM users WHERE";

    private static String GET_TOP_USERS_BY_COUNTRY_SECOND_PART = " ORDER BY country, level DESC, last_level_update_date) " +
            "ranked WHERE country_rank <= " + LEADERBOARD_SIZE + ";";

    public static void getLeaguesData() {

        var stringBuilder = new StringBuilder(GET_TOP_USERS_BY_COUNTRY_FIRST_PART);
        activeCountries.forEach(country -> {
            stringBuilder.append(" country = '").append(country).append("' ").append(" or ");
        });

        stringBuilder.setLength(stringBuilder.length() - 4);
        stringBuilder.append(GET_TOP_USERS_BY_COUNTRY_SECOND_PART);
        var query = stringBuilder.toString();

        try(Connection con = DBConnectionManager.getReadonlyConnection();
            PreparedStatement pst = con.prepareStatement(query);
            ResultSet rs = pst.executeQuery()) {

            while(rs.next()) {
                System.err.println(rs.getLong("user_id"));
                System.err.println(rs.getString("country"));
                System.err.println(rs.getInt("level"));
                System.err.println(rs.getTime("last_level_update_date").getTime());
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
