package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {


        System.out.println(" Cache Client started...");

        Crdt crdt = new Crdt();
        crdt.add("http://localhost:3000");
        crdt.add("http://localhost:3001");
        crdt.add("http://localhost:3002");


        // Read Repair
        crdt.writeToAllNodes(1, "a");
        Thread.sleep(30 * 1000);
        crdt.writeToAllNodes(1, "b");
        Thread.sleep(30 * 1000);
        System.out.println(" servers: " + crdt.readFromAllNodes(1));


        //Write Rollback
        Thread.sleep(30 * 1000);
        crdt.writeToAllNodes(2, "c");


        System.out.println("Exiting Cache Client.");



    }

}

