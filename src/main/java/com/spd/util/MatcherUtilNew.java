package com.spd.util;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;
/**
 * @author he
 */
public class MatcherUtilNew implements Serializable {


    public Map<Character, Map<String,String>> TREE_MAP = new TreeMap<Character, Map<String,String>>();

    public void addPattern(String pattern, String data) {
        if (pattern.length() == 0){
            throw new IllegalArgumentException("empty pattern");
        }
        char c = pattern.charAt(0);
        Map<String,String> n ;
        if (TREE_MAP.containsKey(c)) {
            n = TREE_MAP.get(c);
        } else {
            n = new TreeMap<String,String>();
            n.put(pattern,data);
            TREE_MAP.put(c, n);
        }
        n.put(pattern, data);
    }

    public Map<String,String> matchs_new(String path) {
        Map<String,String> result = new HashMap<String,String>();
        if(null == path || "".equals(path)){
            return null;
        }
        char c = path.toCharArray()[0];
        if(TREE_MAP.containsKey(c)){
            Map<String,String> urls = TREE_MAP.get(c);
            for (String url:urls.keySet()){
                if(isMatch(url,path)){
                    result.put(url,urls.get(url));
                    break;
                }
            }
        }else{
            return null;
        }
        return result;
    }


    private static boolean isMatch(String url, String path) {
        String[] matchstrs = StringUtils.splitPreserveAllTokens(url, "\\*");
        boolean isMatch=false;
        for (String ms : matchstrs) {
            if(path.contains(ms)){
                isMatch=true;
            }else{
                isMatch=false;
                break;
            }
        }
        return isMatch;
    }

    public static void main(String[] args) {
        String path="http://ichannel.snssdk1.com/a/dc/re?appid=124&aid=151071&rowtime=124";
        String a="ichannel.snssdk.com/*aid=151071*";
        System.out.println(isMatch(a,path));

    }



}