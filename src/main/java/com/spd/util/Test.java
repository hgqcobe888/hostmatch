package com.spd.util;

public class Test {

    public static void main(String[] args) {
        MatcherUtil m = new MatcherUtil();
        MatcherUtil.MatchResult[] results = {};


        m.addPattern("isnsk.com".toUpperCase(),"C1");
        //m.addPattern("isnsk.com?aid=1".toUpperCase(),"C0");

        String destination = "http://isnsk.com/ad?index=1530457578&aid=1".toUpperCase() ;
        int index = destination.indexOf("HTTP://");
        if(index!=-1){
            destination = destination.substring(index+7);
        }
        System.out.println(destination);
        results = m.matchs(destination);

        for (MatcherUtil.MatchResult r : results) {
            System.out.println(r.pattern+"##"+r.data);
        }
    }
}
