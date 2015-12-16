package main.java;

public class ConfigurationProvider {

    public String getCustkey() {
        return custkey;
    }

    public String getCustsecret() {
        return custsecret;
    }

    public String getAccesstoken() {
        return accesstoken;
    }

    public String getAccesssecret() {
        return accesssecret;
    }

    String custkey, custsecret;
    String accesstoken, accesssecret;
    public  ConfigurationProvider(){
        custkey = "";
        custsecret="";
        accesstoken = "";
        accesssecret = "";
    }
}