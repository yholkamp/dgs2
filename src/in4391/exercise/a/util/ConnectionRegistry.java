package in4391.exercise.a.util;

import in4391.exercise.a.core.INode;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

/**
 * Simple wrapper to handle communications with multiple RMI registries
 */
public class ConnectionRegistry
{
    private Registry localRegistry;

    private Set<String> connections = new HashSet<String>();

    private Map<String, INode> lookupMap = new HashMap<String, INode>();
    private List<Registry> registries = new ArrayList<Registry>();

    public ConnectionRegistry(Registry localRegistry, String hostname)
    {
        try {
            RMIConfigReader configReader = new RMIConfigReader();
            for(String server : configReader.get("servers").split(",")) {
                if(!server.equals(hostname)) {
                    connections.add(server);
                }
            }
        } catch (IOException e) {
            System.out.println("Failed to read the system config.");
            e.printStackTrace();
            System.exit(1);
        }

        this.localRegistry = localRegistry;
        registries.add(localRegistry);

        // Add the registries by hostname
        for(String connection : connections) {
            try {
                registries.add(LocateRegistry.getRegistry(connection));
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Lists the names of all the registered entities on the various rmiregistries. Also updates a map of String-Stub entities for faster lookups
     * @return
     */
    public List<String> list() {
        List<String> output = new ArrayList<String>();

        for(Registry registry : registries) {
            try {
                for(String node : registry.list()) {
                    lookupMap.put(node, (INode)registry.lookup(node));
                    output.add(node);
                }
            } catch (RemoteException e) {
                e.printStackTrace();
            } catch (NotBoundException e) {
                e.printStackTrace();
            }
        }
        return output;
    }

    /**
     * Binds an object to the local registry.
     * @param id
     * @param myStub
     */
    public void bind(String id, INode myStub) {
        System.out.println("Binding " + id);
        try {
            localRegistry.bind(id, myStub);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (AlreadyBoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the INode of a bound object
     * @param name
     * @return
     */
    public INode lookup(String name) {
        return lookupMap.get(name);
    }

}
