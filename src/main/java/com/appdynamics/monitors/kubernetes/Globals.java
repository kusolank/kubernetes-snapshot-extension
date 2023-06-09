package com.appdynamics.monitors.kubernetes;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

public class Globals {
//    public static DateTime lastElementTimestamp = null;
//    public static DateTime previousRunTimestamp = null;
    public static OffsetDateTime lastElementTimestamp = null;
    public static OffsetDateTime previousRunTimestamp = null;
    public static String lastElementSelfLink = "";
    public static String previousRunSelfLink = "";

    public static long lastDashboardCheck = 0;
    
    public static Map<String, String> NODE_ROLE_MAP = new HashMap<String, String>();
    
    
}
