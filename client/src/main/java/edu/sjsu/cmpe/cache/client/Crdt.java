package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Crdt {

    public ConcurrentHashMap<String, String> writesStatus = new ConcurrentHashMap<String, String>();
    public ConcurrentHashMap<String, String> readStatus = new ConcurrentHashMap<String, String>();
    private ArrayList<DistributedCacheService> servers = new ArrayList<DistributedCacheService>();

    public void add(String serverURL) {
        servers.add(new DistributedCacheService(serverURL, this));
    }


    public void writeToAllNodes(long key, String value) {

        int failures = 0;

        for (DistributedCacheService ser : servers) {
            ser.put(key, value);
        }

        do {

            if (writesStatus.size() >= servers.size()) {

                for (DistributedCacheService server : servers) {
                    System.out.println("Writing to " + server.getCacheServerURL() + ": " + writesStatus.get(server.getCacheServerURL()));
                    if (writesStatus.get(server.getCacheServerURL()).equalsIgnoreCase("fail"))
                        failures++;
                }

                if (failures > 1) {
                    System.out.println("Too many Failures...rollback");
                    for (DistributedCacheService server : servers) {
                        server.delete(key);
                    }
                }
                writesStatus.clear();
                break;
            } else {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } while (true);
    }

    public String readFromAllNodes(long key) throws InterruptedException {

        for (DistributedCacheService server : servers) {
            server.get(key);
        }
        Set<DistributedCacheService> failedServers = new HashSet<DistributedCacheService>();
        Set<DistributedCacheService> consistentServers = new HashSet<DistributedCacheService>(servers);
        consistentServers.addAll(servers);


        while (true) {
            if (readStatus.size() < 3) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {


                System.out.println(readStatus);
                for (DistributedCacheService server : servers) {

                    if (readStatus.get(server.getCacheServerURL()).equalsIgnoreCase("fail")) {
                        System.out.println("Failure at : " + server.getCacheServerURL());
                        failedServers.add(server);
                    }
                }

                consistentServers.removeAll(failedServers);
                System.out.println("consistent :" + consistentServers);
                Thread.sleep(500);
                String valueToAdd = null;

                if (failedServers.size() > 0) {


                    System.out.println("failed SErvers" + failedServers);
                    ArrayList<String> allVAlues = new ArrayList<String>();
                    ArrayList<DistributedCacheService> allServers = new ArrayList<DistributedCacheService>();

                    for (DistributedCacheService consServ : consistentServers) {
                        String temp = consServ.getSync(key);
                        //  System.out.println(temp);
                        allVAlues.add(temp);
                        allServers.add(allVAlues.indexOf(temp), consServ);
                    }


                    //System.out.println("allval" + allVAlues);
                    //get value to store
                    Set<String> unique = new HashSet<String>(allVAlues);
                    //System.out.println(unique);
                    int max = Integer.MIN_VALUE;
                    DistributedCacheService maxServer = null;

                    for (String val : unique) {
                        int currMax = Collections.frequency(allVAlues, val);
                        if (currMax > max) {
                            max = currMax;
                            valueToAdd = val;

                        }
                    }

                    System.out.println("making the servers consistent.");

                    for (DistributedCacheService ser : failedServers) {
                        System.out.println("right value for server: " + ser.getCacheServerURL() + " as: " + valueToAdd);
                        ser.putSync(key, valueToAdd);
                    }
                    failedServers.clear();

                    readStatus.clear();

                    return valueToAdd;
                }
                failedServers.clear();

            }
        }

    }

}
