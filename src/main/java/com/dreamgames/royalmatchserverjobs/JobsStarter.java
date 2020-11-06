package com.dreamgames.royalmatchserverjobs;

import com.dreamgames.royalmatchserverjobs.db.DBConnectionManager;
import com.dreamgames.royalmatchserverjobs.league.FillLeaderboard;
import com.dreamgames.royalmatchserverjobs.util.ServerProperties;

public class JobsStarter {
    public static void main(String[] args) throws Exception {
        var propertiesFilePath = System.getenv().get("PROPERTIES_PATH");
        ServerProperties.init(propertiesFilePath);
        DBConnectionManager.init();
        FillLeaderboard.process();
    }
}
