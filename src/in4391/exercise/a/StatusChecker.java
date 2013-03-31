package in4391.exercise.a;

import in4391.exercise.a.util.ConnectionRegistry;
import in4391.exercise.a.core.INode;

import java.io.*;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.*;

/**
 * CLI application to view or log the various system values such as CPU load, JVM process load and various queue sizes.
 */
public class StatusChecker
{
    /**
     * Starts the monitoring process, using the first parameter as the RMI registry location and the second parameter
     * to indicate whether to log or display the info.
     *
     * @param args
     */
    public static void main(String args[])
    {
        String externalIp = (args.length < 1) ? null : args[0];
        int automatic = (args.length < 2) ? 0 : Integer.valueOf(args[1]);

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }


        ConnectionRegistry registry;
        try {
            registry = new ConnectionRegistry(LocateRegistry.getRegistry(), externalIp);
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        if (automatic == 0) {
            consoleMonitor(registry);
        } else {
            autoMonitor(registry);
        }
    }

    private static void autoMonitor(ConnectionRegistry registry)
    {
        System.out.println("Logging system status");
        File gsLogFile = new File("gs.monitor.log");
        File rmLogFile = new File("rm.monitor.log");
        BufferedWriter gsLogWriter;
        BufferedWriter rmLogWriter;
        FileWriter gsLogFileWriter;
        FileWriter rmLogFileWriter;
        try {
            if (!gsLogFile.exists()) {
                gsLogFile.createNewFile();
            }

            if (!rmLogFile.exists()) {
                rmLogFile.createNewFile();
            }

            gsLogFileWriter = new FileWriter(gsLogFile.getAbsoluteFile(), false);
            gsLogWriter = new BufferedWriter(gsLogFileWriter);

            gsLogWriter.write("name,time,t,message_queue,job_queue,rm_count,cpu_percentage,jvm_percentage\n");

            rmLogFileWriter = new FileWriter(rmLogFile.getAbsoluteFile(), false);
            rmLogWriter = new BufferedWriter(rmLogFileWriter);
            rmLogWriter.write("name,time,message_queue,job_queue,max_queue,free_nodes,total_nodes,cpu_percentage,jvm_percentage\n");

            while (true) {
                writeStats(registry, gsLogWriter, rmLogWriter);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }

            gsLogWriter.close();
            gsLogFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeStats(ConnectionRegistry registry, BufferedWriter gsLogWriter, BufferedWriter rmLogWriter)
    {
        long currentTime = System.currentTimeMillis();
        Map<String, List<Integer>> nodeStates = retrieveStats(registry);

        for(String nodeName : nodeStates.keySet()) {
            if(nodeName.substring(0,2).equals("RM")) {
                // RMNode
                appendLog(rmLogWriter, currentTime, nodeName, nodeStates.get(nodeName));
            } else {
                // GSNode
                appendLog(gsLogWriter, currentTime, nodeName, nodeStates.get(nodeName));
            }
        }
    }

    private static void appendLog(BufferedWriter logWriter, long currentTime, String nodeName, List<Integer> states)
    {
        StringBuffer sb = new StringBuffer(nodeName+","+currentTime);
        for(int i : states) {
            sb.append("," + i);
        }
        sb.append("\n");
        try {
            logWriter.write(sb.toString());
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void consoleMonitor(ConnectionRegistry registry)
    {
        // Set up console reading
        String line;
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        System.out.println("Hit <return> to retrieve the system state.\n");

        // Wait for user input
        while (true) {
            try {
                line = in.readLine();
                if (line == null || line.equals("q"))
                    break; // Quit

                printStats(registry);
            } catch (IOException e) {
                System.err.println("Server exception: " + e.toString());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Continuing");
            }
        }

        System.exit(0);
    }

    private static void printStats(ConnectionRegistry registry)
    {
        Map<String, List<Integer>> nodeStates = retrieveStats(registry);
        System.out.println("Found " + nodeStates.size() + " nodes");

        System.out.println("GSNode\t\t  t\t MsgQ\t JobQ\t# RMs\t CPU%\t JVM%");
        for(String nodeName : nodeStates.keySet()) {
            if(nodeName.substring(0,2).equals("GS")) {
                printState(nodeName, nodeStates.get(nodeName));
            }
        }
        System.out.println();

        System.out.println("RMNode\t\t MsgQ\t JobQ\t maxQ\t#freeN\tnodes\t CPU%\t JVM%");
        for(String nodeName : nodeStates.keySet()) {
            if(nodeName.substring(0,2).equals("RM")) {
                printState(nodeName, nodeStates.get(nodeName));
            }
        }

        System.out.println("********************************************************");
    }

    private static Map<String, List<Integer>> retrieveStats(ConnectionRegistry registry)
    {
        Map<String, List<Integer>> nodeStates = new HashMap<String, List<Integer>>();

        try {
            for (String id : registry.list()) {
                INode obj = registry.lookup(id);
                int[] rawStates = obj.getNodeState();

                List<Integer> states = new ArrayList<Integer>(rawStates.length);
                for (int i : rawStates)
                    states.add(i);

                nodeStates.put(id, states);
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        return nodeStates;
    }

    private static void printState(String id, List<Integer> states)
    {
        System.out.print(id + "\t");
        for (int i : states) {
            System.out.print(String.format("% 5d", i) + "\t");
        }
        System.out.println();
    }
}